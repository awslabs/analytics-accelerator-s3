/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.benchmarks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.access.StreamRead;
import software.amazon.s3.analyticsaccelerator.access.StreamReadPattern;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.common.ObjectRange;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * Micro benchmark which runs concurrent streams on a single node, and mimics parquet column +
 * footer reads for a large number of parquet objects. This is a useful indicator of how queries
 * might perform in TPC-DS benchmarks.
 */
public class ConcurrentStreamPerformanceBenchmark {

  public static final String DATASET_KEY = "S3_DATASET_BUCKET";
  public static final String PREFIX_KEY = "S3_TEST_PREFIX";

  /** This class holds the common variables to be used across micro benchmarks in this class. */
  @State(Scope.Thread)
  public static class BenchmarkState {
    S3Client s3Client;
    S3AsyncClient s3AsyncClient;
    List<S3Object> s3Objects;
    ExecutorService executor;
    ConnectorConfiguration configuration;
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory;
    String bucketName;
    int maxConcurrency;

    @Param public S3ClientAndReadKind clientKind;

    /**
     * Set up before running micro benchmarks. Useful for building initial state, time taken here
     * will not be included in the micro benchmark report.
     */
    @Setup
    public void setup() {
      this.s3AsyncClient =
          S3AsyncClient.builder()
              .httpClient(NettyNioAsyncHttpClient.builder().maxConcurrency(400).build())
              .region(Region.US_EAST_1)
              .build();

      this.s3Client =
          S3Client.builder()
              .httpClient(ApacheHttpClient.builder().maxConnections(400).build())
              .region(Region.US_EAST_1)
              .build();

      // The number of reads to do in parallel
      this.maxConcurrency = Runtime.getRuntime().availableProcessors();
      this.executor = Executors.newFixedThreadPool(100);
      this.configuration = new ConnectorConfiguration(System.getenv());
      this.bucketName = this.configuration.getRequiredString(DATASET_KEY);
      this.s3Objects =
          BenchmarkUtils.getKeys(
              s3Client, bucketName, configuration.getRequiredString(PREFIX_KEY), 200);
      this.s3SeekableInputStreamFactory =
          new S3SeekableInputStreamFactory(
              new S3SdkObjectClient(this.s3AsyncClient),
              S3SeekableInputStreamConfiguration.DEFAULT);
    }

    /** Shut down once all micro benchmarks in this class complete. */
    @TearDown
    public void tearDown() throws IOException {
      executor.shutdownNow();
      s3SeekableInputStreamFactory.close();
    }
  }

  @Benchmark
  @Warmup(iterations = 1)
  @Measurement(iterations = 3)
  @Fork(1)
  @BenchmarkMode(Mode.SingleShotTime)
  public void runBenchmark(BenchmarkState state) throws Exception {
    execute(state, state.configuration.getRequiredString(DATASET_KEY));
  }

  private void execute(BenchmarkState state, String bucket) throws Exception {
    System.out.println(
        "\nReading parquet files with: " + state.clientKind + " from bucket: " + bucket);

    for (int i = 0; i < state.s3Objects.size() - 1; i = i + state.maxConcurrency) {
      List<CompletableFuture<?>> futures = new ArrayList<>();

      for (int j = i; j < i + state.maxConcurrency && j < state.s3Objects.size() - 1; j++) {
        final int k = j;
        CompletableFuture<?> f =
            CompletableFuture.runAsync(
                () -> {
                  try {
                    if (state.clientKind == S3ClientAndReadKind.AAL_ASYNC_READ_VECTORED) {
                      fetchObjectsFromAAL(bucket, state.s3Objects.get(k), state);
                    } else {
                      fetchObjectChunksByRange(bucket, state.s3Objects.get(k), state);
                    }
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                },
                state.executor);
        futures.add(f);
      }

      CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).get();
    }
  }

  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
      value = "RR_NOT_CHECKED",
      justification = "ok to ignore return value from the read here")
  private void fetchObjectsFromAAL(String bucketName, S3Object s3Object, BenchmarkState state)
      throws InterruptedException, ExecutionException, IOException {

    StreamReadPattern streamReadPattern =
        BenchmarkUtils.getQuasiParquetColumnChunkPattern(s3Object.size());

    List<ObjectRange> objectRanges = new ArrayList<>();

    for (StreamRead streamRead : streamReadPattern.getStreamReads()) {
      objectRanges.add(
          new ObjectRange(
              new CompletableFuture<>(), streamRead.getStart(), (int) streamRead.getLength()));
    }

    S3SeekableInputStream inputStream =
        state.s3SeekableInputStreamFactory.createStream(
            S3URI.of(bucketName, s3Object.key()),
            OpenStreamInformation.builder()
                .objectMetadata(
                    ObjectMetadata.builder()
                        .contentLength(s3Object.size())
                        .etag(s3Object.eTag())
                        .build())
                .build());

    // Read last 8 bytes
    StreamRead magicBytesRead = BenchmarkUtils.getMagicBytesRead(s3Object.size());
    byte[] magicBytesBuffer = new byte[(int) magicBytesRead.getLength()];
    inputStream.read(magicBytesBuffer, 0, (int) magicBytesRead.getLength());

    // Read footer
    StreamRead footerRead = BenchmarkUtils.getFooterRead(s3Object.size());
    byte[] footerReadBuffer = new byte[(int) footerRead.getLength()];
    inputStream.read(footerReadBuffer, 0, (int) footerRead.getLength());

    inputStream.readVectored(
        objectRanges,
        ByteBuffer::allocate,
        (buffer) -> {
          System.out.println("Do nothing on the release!");
        });

    for (ObjectRange objectRange : objectRanges) {
      objectRange.getByteBuffer().get();
    }
  }

  private void fetchObjectChunksByRange(String bucket, S3Object s3Object, BenchmarkState state)
      throws Exception {

    StreamRead magicBytesRead = BenchmarkUtils.getMagicBytesRead(s3Object.size());
    readStream(
        getDataStream(bucket, s3Object, state, magicBytesRead), s3Object.key(), magicBytesRead);

    StreamRead footerRead = BenchmarkUtils.getFooterRead(s3Object.size());
    readStream(getDataStream(bucket, s3Object, state, footerRead), s3Object.key(), footerRead);

    StreamReadPattern streamReadPattern =
        BenchmarkUtils.getQuasiParquetColumnChunkPattern(s3Object.size());

    List<CompletableFuture<Long>> futures = new ArrayList<>();

    for (StreamRead streamRead : streamReadPattern.getStreamReads()) {
      futures.add(
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return readStream(
                      getDataStream(bucket, s3Object, state, streamRead),
                      s3Object.key(),
                      streamRead);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              },
              state.executor));
    }

    CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).get();
  }

  private ResponseInputStream<GetObjectResponse> getDataStream(
      String bucket, S3Object s3Object, BenchmarkState state, StreamRead streamRead) {
    GetObjectRequest request =
        GetObjectRequest.builder()
            .bucket(bucket)
            .key(s3Object.key())
            .range(
                String.format(
                    "bytes=%s-%s",
                    streamRead.getStart(), streamRead.getStart() + streamRead.getLength() - 1))
            .build();

    ResponseInputStream<GetObjectResponse> dataStream;

    if (state.clientKind == S3ClientAndReadKind.SDK_ASYNC_JAVA) {
      dataStream =
          state
              .s3AsyncClient
              .getObject(request, AsyncResponseTransformer.toBlockingInputStream())
              .join();
    } else if (state.clientKind == S3ClientAndReadKind.SDK_SYNC_JAVA) {
      dataStream = state.s3Client.getObject(request);
    } else {
      dataStream = null;
    }

    return dataStream;
  }

  private long readStream(
      ResponseInputStream<GetObjectResponse> inputStream, String key, StreamRead streamRead)
      throws Exception {

    byte[] buffer = new byte[(int) streamRead.getLength()];

    long read = inputStream.read(buffer);
    inputStream.close();

    return read;
  }
}

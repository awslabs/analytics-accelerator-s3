package software.amazon.s3.analyticsaccelerator.benchmarks;


import lombok.SneakyThrows;
import org.openjdk.jmh.annotations.*;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.endpoints.internal.Value;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.access.S3ClientKind;
import software.amazon.s3.analyticsaccelerator.access.S3ExecutionConfiguration;
import software.amazon.s3.analyticsaccelerator.access.S3ExecutionContext;
import software.amazon.s3.analyticsaccelerator.common.ObjectRange;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

public class ConcurrentStreamPerformanceBenchmark {

    @State(Scope.Thread)
    public static class BenchmarkState {
        S3Client s3Client;
        S3AsyncClient s3AsyncClient;
        List<S3Object> s3Objects;
        ExecutorService executor;
        S3ExecutionContext s3ExecutionContext;
        S3SeekableInputStreamFactory s3SeekableInputStreamFactory;
        String bucketName;
        int maxConcurrency;

        @Param({"ASYNC_JAVA", "SYNC_JAVA", "AAL_READ_VECTORED"})
        public String clientKind;


        @Setup
        public void setup() {
            this.s3AsyncClient = S3AsyncClient.builder()
                    .httpClient(NettyNioAsyncHttpClient.builder().maxConcurrency(400).build())
                    .region(Region.US_EAST_1).build();

            this.s3Client = S3Client.builder()
                    .httpClient(ApacheHttpClient.builder().maxConnections(400).build())
                    .region(Region.US_EAST_1).build();

            // The number of reads to do in parallel
            this.maxConcurrency = Runtime.getRuntime().availableProcessors();
            this.executor = Executors.newFixedThreadPool(100);
            this.s3ExecutionContext = new S3ExecutionContext(S3ExecutionConfiguration.fromEnvironment());
            this.bucketName = s3ExecutionContext.getConfiguration().getAsyncBucket();
            this.s3Objects = getKeys(s3Client, bucketName, s3ExecutionContext.getConfiguration().getPrefix());
            this.s3SeekableInputStreamFactory = new S3SeekableInputStreamFactory(new S3SdkObjectClient(this.s3AsyncClient), S3SeekableInputStreamConfiguration.DEFAULT);
        }

        @TearDown
        public void tearDown() {
            executor.shutdown();
        }

        private List<S3Object> getKeys(S3Client s3Client, String bucket, String prefix) {
            ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(bucket).prefix(prefix)
                    .maxKeys(500);

            ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());

            System.out.println("\nObjects fetched: " + response.contents().size());

            return response.contents();
        }
    }

    @Benchmark
    @Measurement(iterations = 3)
    @Fork(1)
    @BenchmarkMode(Mode.SingleShotTime)
    public void runBenchmark(BenchmarkState state) throws Exception {
        switch(state.clientKind) {
            case "ASYNC_JAVA":
                execute(state, state.s3ExecutionContext.getConfiguration().getAsyncBucket());
                break;
            case "SYNC_JAVA":
                execute(state, state.s3ExecutionContext.getConfiguration().getSyncBucket());
                break;
            case "AAL_READ_VECTORED":
                execute(state, state.s3ExecutionContext.getConfiguration().getVectoredBucket());
                break;
        }

    }

    private void execute(BenchmarkState state, String bucket) throws Exception {
        System.out.println("\nReading parquet files with: " + state.clientKind + " from bucket: " + bucket);

        for (int i = 0; i < state.s3Objects.size() - 1; i = i + state.maxConcurrency) {
            List<Future<Integer>> futures = new ArrayList<>();

            for (int j = i; j < i + state.maxConcurrency && j < state.s3Objects.size() - 1; j++) {
                final int k = j;
                    Future<Integer> f = state.executor.submit(() -> {
                        try {
                            if (Objects.equals(state.clientKind, "AAL_READ_VECTORED")) {
                                return featchObjectsFromAAL(bucket, state.s3Objects.get(k), state);
                            } else {
                                return fetchObjectChunksByRange(bucket, state.s3Objects.get(k), state);
                            }
                        } catch (ExecutionException | InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    futures.add(f);
            }

            for(Future<Integer> f : futures) {
                f.get();
            }

          //  System.out.printf("DONE READING FOR ITERATION <<<< " + i);
        }
    }

    private int featchObjectsFromAAL(String bucketName, S3Object s3Object, BenchmarkState state) throws InterruptedException, ExecutionException, IOException {
        long objSize = s3Object.size();

        if (objSize < 90 * ONE_MB) {
            return -1;
        }

        List<ObjectRange> objectRanges = new ArrayList<>();
        objectRanges.add(new ObjectRange(new CompletableFuture<>(), objSize - ONE_MB, ONE_MB));
        objectRanges.add(new ObjectRange(new CompletableFuture<>(), 4, 4 * ONE_MB));
        objectRanges.add(new ObjectRange(new CompletableFuture<>(), 30 * ONE_MB, 8 * ONE_MB));
        objectRanges.add(new ObjectRange(new CompletableFuture<>(), 50 * ONE_MB, 12 * ONE_MB));
        objectRanges.add(new ObjectRange(new CompletableFuture<>(), 70 * ONE_MB, 10 * ONE_MB));

        S3SeekableInputStream inputStream = state.s3SeekableInputStreamFactory.createStream(S3URI.of(bucketName, s3Object.key()),
                OpenStreamInformation.builder()
                        .objectMetadata(
                                ObjectMetadata
                                        .builder()
                                        .contentLength(s3Object.size())
                                        .etag(s3Object.eTag()).build()).build());

        inputStream.readVectored(objectRanges, ByteBuffer::allocate, (buffer) -> {
            System.out.println("Do nothing on the release!");
        });

        for (ObjectRange objectRange : objectRanges) {
            objectRange.getByteBuffer().get();
        }

        return 0;
    }

    private int fetchObjectChunksByRange(String bucket, S3Object s3Object, BenchmarkState state) throws ExecutionException, InterruptedException {
        long objSize = s3Object.size();

        if (objSize < 80 * ONE_MB) {
            return -1;
        }

        List<Range> ranges = new ArrayList<>();
        ranges.add(new Range(objSize - ONE_MB, objSize));
        ranges.add(new Range(4, 4 + 4 * ONE_MB));
        ranges.add(new Range(30 * ONE_MB, 30 * ONE_MB + 8 * ONE_MB));
        ranges.add(new Range(50 * ONE_MB, 50 * ONE_MB + 12 * ONE_MB));
        ranges.add(new Range(70 * ONE_MB, 70 * ONE_MB + 10 * ONE_MB));

        List<Future<Long>> fList = new ArrayList<>();

        for (Range range : ranges) {

            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(s3Object.key())
                    .range("bytes="+range.getStart() + "-" + range.getEnd()).build();

          //  System.out.println("\nMaking a GET request for: " + s3Object.key() + " start: " + range.getStart() + " end: " + range.getEnd());

            ResponseInputStream<GetObjectResponse> dataStream;

            if (Objects.equals(state.clientKind, "ASYNC_JAVA")) {
                dataStream = state.s3AsyncClient.getObject(request,
                        AsyncResponseTransformer.toBlockingInputStream()).join();
            } else if (Objects.equals(state.clientKind, "SYNC_JAVA")) {
                dataStream = state.s3Client.getObject(request);
            } else {
                dataStream = null;
            }

            fList.add(state.executor.submit(() -> readStream(dataStream, s3Object.key(), range)));
        }

        for(Future<Long> f : fList) {
            f.get();
        }

        return 0;
    }


    private long readStream(ResponseInputStream<GetObjectResponse> inputStream, String key, Range range) throws Exception {
        byte[] buffer = new byte[range.getLength()];
        long read = 0;
        try {
            read = inputStream.read(buffer);
           // System.out.printf("\nDone reading for: " + key + " start: " + range.getStart() + " end: " + range.getEnd());
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return read;
    }

}

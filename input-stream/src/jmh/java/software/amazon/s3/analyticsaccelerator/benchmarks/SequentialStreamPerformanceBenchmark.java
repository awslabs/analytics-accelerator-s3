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

import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_KB;

import java.io.IOException;
import java.io.InputStream;
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
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.S3SyncSdkObjectClient;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class SequentialStreamPerformanceBenchmark {

  public static final String DATASET_KEY = "S3_DATASET_BUCKET";
  public static final String OBJECT_KEY = "S3_OBJECT_KEY";

  /** This class holds the common variables to be used across micro benchmarks in this class. */
  @State(Scope.Thread)
  public static class BenchmarkState {
    S3Client s3Client;
    S3AsyncClient s3AsyncClient;
    ConnectorConfiguration configuration;
    S3SeekableInputStreamFactory s3SeekableInputStreamFactory;
    String bucketName;
    String objectKey;
    Long objectSize;

    @Param({"AAL", "SDK"})
    public String clientKind;

    /**
     * Set up before running micro benchmarks. Useful for building initial state, time taken here
     * will not be included in the micro benchmark report.
     */
    @Setup
    public void setup() {
      this.s3Client =
          S3Client.builder()
              .httpClient(ApacheHttpClient.builder().maxConnections(400).build())
              .region(Region.EU_WEST_1)
              .build();

      this.configuration = new ConnectorConfiguration(System.getenv());
      this.bucketName = this.configuration.getRequiredString(DATASET_KEY);
      this.objectKey = this.configuration.getRequiredString(OBJECT_KEY);
      this.s3SeekableInputStreamFactory =
          new S3SeekableInputStreamFactory(
              new S3SyncSdkObjectClient(this.s3Client), S3SeekableInputStreamConfiguration.DEFAULT);
      this.objectSize = getObjectLength(s3Client, bucketName, objectKey);
    }

    /** Shut down once all micro benchmarks in this class complete. */
    @TearDown
    public void tearDown() throws IOException {
      s3SeekableInputStreamFactory.close();
    }

    private long getObjectLength(S3Client s3Client, String bucket, String key) {
      HeadObjectResponse headObjectResponse =
          s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());

      return headObjectResponse.contentLength();
    }
  }

  @Benchmark
  @Warmup(iterations = 1)
  @Measurement(iterations = 3)
  @Fork(1)
  @BenchmarkMode(Mode.SingleShotTime)
  public void runBenchmark(BenchmarkState state) throws Exception {
    execute(state);
  }

  private void execute(BenchmarkState state) throws Exception {
    if (state.clientKind.equals("SDK")) {
      readWithSDK(state);
    } else if (state.clientKind.equals("AAL")) {
      readWithAAL(state);
    }
  }

  private void readWithAAL(BenchmarkState state) throws Exception {
    System.out.println(
        "Reading object: " + state.objectKey + "from bucket: " + state.bucketName + " with AAL");

    S3SeekableInputStream s3SeekableInputStream =
        state.s3SeekableInputStreamFactory.createStream(
            S3URI.of(state.bucketName, state.objectKey));

    drainStream(s3SeekableInputStream, state.objectSize);
  }

  private void readWithSDK(BenchmarkState state) throws Exception {
    System.out.println(
        "Reading object: " + state.objectKey + "from bucket: " + state.bucketName + "with SDK");

    GetObjectRequest request =
        GetObjectRequest.builder()
            .bucket(state.bucketName)
            .key(state.objectKey)
            .range(String.format("bytes=%s-%s", 0, state.objectSize - 1))
            .build();

    ResponseInputStream<GetObjectResponse> dataStream = state.s3Client.getObject(request);
    drainStream(dataStream, state.objectSize);
  }

  private void drainStream(InputStream inputStream, long objectSize) throws IOException {
    // 8KB
    int readSize = 8 * ONE_KB;
    byte[] data = new byte[readSize];
    long bytesToRead = objectSize;

    while (bytesToRead > 0) {
      int bytesRead = inputStream.read(data, 0, readSize);

      // EOF
      if (bytesRead == -1) {
        return;
      }

      bytesToRead -= bytesRead;
    }
  }
}

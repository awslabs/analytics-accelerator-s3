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
package software.amazon.s3.analyticsaccelerator.access;

import static software.amazon.s3.analyticsaccelerator.access.SizeConstants.ONE_MB_IN_BYTES;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/** Configuration for benchmarks */
@Value
@Builder
public class S3ExecutionConfiguration {
  public static final String BUCKET_KEY = "S3_TEST_BUCKET";
  public static final String PREFIX_KEY = "S3_TEST_PREFIX";
  public static final String READ_BUFFER_SIZE_MB_KEY = "S3_TEST_READ_BUFFER_SIZE_MB";
  public static final int DEFAULT_READ_BUFFER_SIZE_MB_KEY = 8;
  public static final String BUCKET_KEY_ASYNC = "S3_TEST_BUCKET_ASYNC";
  public static final String BUCKET_KEY_SYNC = "S3_TEST_BUCKET_SYNC";
  public static final String BUCKET_KEY_VECTORED = "S3_TEST_BUCKET_VECTORED";
  public static final int DEFAULT_PARQUET_BENCHMARK_BUCKET_KEY = 8;

  @NonNull String bucket;
  @NonNull String prefix;
  @NonNull String asyncBucket;
  @NonNull String syncBucket;
  @NonNull String vectoredBucket;
  int bufferSizeMb;
  @NonNull S3AsyncClientFactoryConfiguration clientFactoryConfiguration;

  /**
   * Creates the {@link S3ExecutionConfiguration} from the supplied configuration
   *
   * @param configuration an instance of configuration
   * @return anew instance of {@link S3AsyncClientFactoryConfiguration}
   */
  public static S3ExecutionConfiguration fromConfiguration(ConnectorConfiguration configuration) {
    return S3ExecutionConfiguration.builder()
        .bucket(configuration.getRequiredString(BUCKET_KEY))
        .asyncBucket(configuration.getRequiredString(BUCKET_KEY_ASYNC))
        .asyncBucket(configuration.getRequiredString(BUCKET_KEY_ASYNC))
        .syncBucket(configuration.getRequiredString(BUCKET_KEY_SYNC))
        .vectoredBucket(configuration.getRequiredString(BUCKET_KEY_VECTORED))
        .prefix(configuration.getRequiredString(PREFIX_KEY))
        .bufferSizeMb(
            configuration.getInt(READ_BUFFER_SIZE_MB_KEY, DEFAULT_READ_BUFFER_SIZE_MB_KEY))
        .clientFactoryConfiguration(
            S3AsyncClientFactoryConfiguration.fromConfiguration(configuration))
        .build();
  }

  /**
   * Returns the base Uri
   *
   * @return base Uri
   */
  public S3URI getBaseUri() {
    return S3URI.of(this.getBucket(), this.getPrefix());
  }

  /**
   * Returns the buffer size in bytes
   *
   * @return buffer size in bytes
   */
  public int getBufferSizeBytes() {
    return ONE_MB_IN_BYTES * this.getBufferSizeMb();
  }

  /**
   * Creates the {@link S3ExecutionConfiguration} from the environment
   *
   * @return anew instance of {@link S3ExecutionConfiguration}
   */
  public static S3ExecutionConfiguration fromEnvironment() {
    return fromConfiguration(new ConnectorConfiguration(System.getenv()));
  }
}

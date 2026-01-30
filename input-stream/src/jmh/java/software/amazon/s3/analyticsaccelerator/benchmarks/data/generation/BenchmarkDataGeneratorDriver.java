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
package software.amazon.s3.analyticsaccelerator.benchmarks.data.generation;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.access.S3AsyncClientFactoryConfiguration;
import software.amazon.s3.analyticsaccelerator.access.S3ExecutionConfiguration;
import software.amazon.s3.analyticsaccelerator.access.S3ExecutionContext;
import software.amazon.s3.analyticsaccelerator.access.S3Object;
import software.amazon.s3.analyticsaccelerator.access.S3ObjectKind;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * This class implements data generation for the sequential read micro-benchmarks. This allows for
 * deterministic data generation which in turn allows to run reproducible micro-benchmarks. The
 * results of micro benchmarks are not to be compared across different computers (Mac of engineer A
 * and DevDesk of engineer B will have different results), but runs on the same computer should be
 * comparable.
 */
public class BenchmarkDataGeneratorDriver {

  private static final String CUSTOMER_KEY = System.getenv("CUSTOMER_KEY");
  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkDataGeneratorDriver.class);

  /**
   * This is the entry point to the generation code. ALl the context is extracted from the
   * Environment variables via {@link S3ExecutionConfiguration#fromEnvironment()}. The list of
   * relevant variable can be found in both {@link S3ExecutionConfiguration} and {@link
   * S3AsyncClientFactoryConfiguration}
   *
   * <p>The mandatory variables are:
   *
   * <ul>
   *   <li>`S3_TEST_BUCKET` - the bucket to generate data into
   *   <li>`S3_TEST_PREFIX` - the root prefix to generate data into
   *   <li>`S3_TEST_REGION` - the region the bucket is in
   * </ul>
   *
   * <p>The rest allows to fine tune the CRT, buffer sizes and so on and are set to sensible
   * defaults.
   *
   * @param args program arguments are currently ignored
   */
  public static void main(String[] args) throws IOException {
    System.out.println("Starting data generation...");
    try (S3ExecutionContext s3ExecutionContext =
        new S3ExecutionContext(S3ExecutionConfiguration.fromEnvironment())) {
      // Output the configuration
      System.out.println(s3ExecutionContext.getConfiguration());

      // For each object, generate the data
      for (S3Object s3Object : S3Object.values()) {
        // Build the Url
        S3ObjectKind s3ObjectKind = s3Object.getKind();
        if ((s3ObjectKind.equals(S3ObjectKind.RANDOM_SEQUENTIAL_ENCRYPTED)
                || s3ObjectKind.equals(S3ObjectKind.RANDOM_PARQUET_ENCRYPTED))
            && (CUSTOMER_KEY == null || CUSTOMER_KEY.trim().isEmpty())) {
          LOG.warn("Skipping {} as CUSTOMER_KEY is not set", s3ObjectKind);
          continue;
        }

        S3URI s3URI = s3Object.getObjectUri(s3ExecutionContext.getConfiguration().getBaseUri());

        // Create the generator
        BenchmarkObjectGenerator benchmarkObjectGenerator =
            createGenerator(s3ExecutionContext, s3Object.getKind());

        // Generate the data
        benchmarkObjectGenerator.generate(s3URI, s3Object.getSize());
      }
    }
  }

  /**
   * Creates a generator for a given context and generator kind
   *
   * @param context generator context
   * @param s3ObjectKind generator s3ObjectKind S3 Object Kind
   * @return a new instance of {@link BenchmarkObjectGenerator}
   */
  private static BenchmarkObjectGenerator createGenerator(
      S3ExecutionContext context, S3ObjectKind s3ObjectKind) {
    switch (s3ObjectKind) {
      case RANDOM_SEQUENTIAL_ENCRYPTED:
        return new RandomSequentialObjectGenerator(
            context, S3ObjectKind.RANDOM_SEQUENTIAL_ENCRYPTED);
      case RANDOM_PARQUET_ENCRYPTED:
        return new ParquetObjectGenerator(context, S3ObjectKind.RANDOM_PARQUET_ENCRYPTED);
      case RANDOM_PARQUET:
        return new ParquetObjectGenerator(context, S3ObjectKind.RANDOM_PARQUET);
      case RANDOM_SEQUENTIAL:
        return new RandomSequentialObjectGenerator(context, S3ObjectKind.RANDOM_SEQUENTIAL);
      default:
        throw new IllegalArgumentException("Unsupported kind: " + s3ObjectKind);
    }
  }
}

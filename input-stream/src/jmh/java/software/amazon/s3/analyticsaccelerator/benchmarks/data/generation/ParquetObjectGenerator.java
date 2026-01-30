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

import java.io.File;
import lombok.NonNull;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.s3.analyticsaccelerator.access.S3ExecutionContext;
import software.amazon.s3.analyticsaccelerator.access.S3ObjectKind;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * Uploads parquet files present in the resources folder to S3 to allow running of tests involving
 * parquet data . It uses {@link S3TransferManager} backed by the CRT to upload data.
 */
public class ParquetObjectGenerator extends BenchmarkObjectGenerator {

  /**
   * Creates an instance of parquet object generator
   *
   * @param context an instance of {@link S3ExecutionContext}
   * @param kind S3 Object Kind
   */
  public ParquetObjectGenerator(@NonNull S3ExecutionContext context, @NonNull S3ObjectKind kind) {
    super(context, kind);
  }

  @Override
  public void generate(S3URI s3URI, long size) {
    String fileName = s3URI.getKey().substring(s3URI.getKey().lastIndexOf('/') + 1);
    File sourceFile = new File("input-stream/src/jmh/resources/" + fileName);
    if (!sourceFile.exists()) {
      sourceFile = new File("input-stream/src/integrationTest/resources/parquet/" + fileName);
    }
    String progressPrefix = createProgressPrefix(s3URI);
    System.out.println(progressPrefix + "Starting upload from: " + sourceFile.getAbsolutePath());

    try {
      performFileUpload(s3URI, sourceFile.toPath(), "application/x-parquet", sourceFile.length());
    } catch (RuntimeException e) {
      throw new RuntimeException("Failed to upload file", e);
    }
  }

  @Override
  protected S3ObjectKind getEncryptedKind() {
    return S3ObjectKind.RANDOM_PARQUET_ENCRYPTED;
  }
}

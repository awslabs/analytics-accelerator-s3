package com.amazon.connector.s3.datagen;

import com.amazon.connector.s3.datagen.BenchmarkData.BenchmarkObject;
import java.util.Arrays;
import java.util.Random;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.internal.crt.S3CrtAsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * This class implements data generation for the sequential read micro-benchmarks. This allows for
 * deterministic data generation which in turn allows to run reproducible micro-benchmarks. The
 * results of microbenchmarks are not to be compared across different computers (Mac of engineer A
 * and DevDesk of engineer B will have different results), but runs on the same computer should be
 * comparable.
 */
public class SequentialReadDataGenerator {

  /**
   * Entry point: set bucket name and prefix in {@link Constants} to generate a dataset of random
   * objects
   *
   * @param args program arguments are currently ignored
   */
  public static void main(String[] args) {
    Arrays.stream(BenchmarkObject.values()).forEach(SequentialReadDataGenerator::generateObject);
    System.out.println("Done.");
  }

  private static void generateObject(BenchmarkObject benchmarkObject) {
    String key = Constants.BENCHMARK_DATA_PREFIX_SEQUENTIAL + benchmarkObject.getKeyName();
    String fullKeyName = String.format("s3://%s/%s", Constants.BENCHMARK_BUCKET, key);
    System.out.println("Generating " + fullKeyName + " and uploading it to S3...");

    try (S3AsyncClient s3AsyncClient = S3CrtAsyncClient.builder().maxConcurrency(300).build()) {
      s3AsyncClient
          .putObject(
              PutObjectRequest.builder().bucket(Constants.BENCHMARK_BUCKET).key(key).build(),
              AsyncRequestBody.fromBytes(generateBytes(benchmarkObject.getSize())))
          .join();
    }
  }

  private static byte[] generateBytes(long len) {
    byte[] buf = new byte[(int) len];
    Random random = new Random();
    random.nextBytes(buf);
    return buf;
  }
}

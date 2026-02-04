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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/** Represents a singular object to run benchmarks against */
@AllArgsConstructor
@Getter
public enum S3Object {
  RANDOM_1MB("random-1mb.bin", 1 * SizeConstants.ONE_MB_IN_BYTES, S3ObjectKind.RANDOM_SEQUENTIAL),
  RANDOM_4MB("random-4mb.bin", 4 * SizeConstants.ONE_MB_IN_BYTES, S3ObjectKind.RANDOM_SEQUENTIAL),
  RANDOM_16MB(
      "random-16mb.bin", 16 * SizeConstants.ONE_MB_IN_BYTES, S3ObjectKind.RANDOM_SEQUENTIAL),
  RANDOM_64MB(
      "random-64mb.bin", 64 * SizeConstants.ONE_MB_IN_BYTES, S3ObjectKind.RANDOM_SEQUENTIAL),
  RANDOM_128MB(
      "random-128mb.bin", 128 * SizeConstants.ONE_MB_IN_BYTES, S3ObjectKind.RANDOM_SEQUENTIAL),
  RANDOM_256MB(
      "random-256mb.bin", 256 * SizeConstants.ONE_MB_IN_BYTES, S3ObjectKind.RANDOM_SEQUENTIAL),
  RANDOM_512MB(
      "random-512mb.bin", 512 * SizeConstants.ONE_MB_IN_BYTES, S3ObjectKind.RANDOM_SEQUENTIAL),
  RANDOM_1GB("random-1gb.bin", SizeConstants.ONE_GB_IN_BYTES, S3ObjectKind.RANDOM_SEQUENTIAL),
  RANDOM_5GB("random-5gb.bin", 5L * SizeConstants.ONE_GB_IN_BYTES, S3ObjectKind.RANDOM_SEQUENTIAL),
  RANDOM_10GB(
      "random-10gb.bin", 10L * SizeConstants.ONE_GB_IN_BYTES, S3ObjectKind.RANDOM_SEQUENTIAL),
  CSV_1MB("sequential-1mb.csv", 1 * SizeConstants.ONE_MB_IN_BYTES, S3ObjectKind.RANDOM_SEQUENTIAL),
  JSON_1MB(
      "sequential-1mb.json", 1 * SizeConstants.ONE_MB_IN_BYTES, S3ObjectKind.RANDOM_SEQUENTIAL),
  CSV_20MB(
      "sequential-20mb.csv", 20 * SizeConstants.ONE_MB_IN_BYTES, S3ObjectKind.RANDOM_SEQUENTIAL),
  TXT_16MB(
      "sequential-16mb.txt", 16 * SizeConstants.ONE_MB_IN_BYTES, S3ObjectKind.RANDOM_SEQUENTIAL),
  RANDOM_SSEC_ENCRYPTED_SEQUENTIAL_1MB(
      "random-encrypted-1mb.bin",
      SizeConstants.ONE_MB_IN_BYTES,
      S3ObjectKind.RANDOM_SEQUENTIAL_ENCRYPTED),
  RANDOM_SSEC_ENCRYPTED_PARQUET_1MB(
      "random-encrypted-1mb.parquet",
      SizeConstants.ONE_MB_IN_BYTES,
      S3ObjectKind.RANDOM_PARQUET_ENCRYPTED),
  RANDOM_SSEC_ENCRYPTED_PARQUET_10MB(
      "random-encrypted-10mb.parquet",
      10 * SizeConstants.ONE_MB_IN_BYTES,
      S3ObjectKind.RANDOM_PARQUET_ENCRYPTED),

  VALID_SMALL_PARQUET("valid_small.parquet", 454_233, S3ObjectKind.RANDOM_PARQUET),
  CORRUPTED_DICTIONARY_PARQUET("corrupted_dict.parquet", 2_850, S3ObjectKind.RANDOM_PARQUET),
  CORRUPTED_SCHEMA_PARQUET("corrupted_footer.parquet", 451, S3ObjectKind.RANDOM_PARQUET),
  MULTI_ROW_GROUP_PARQUET("multi_row_group.parquet", 2_048, S3ObjectKind.RANDOM_PARQUET),
  SCHEMA_MATCH_FILE1_PARQUET(
      "large_file1.parquet", 7 * SizeConstants.ONE_MB_IN_BYTES, S3ObjectKind.RANDOM_PARQUET),
  SCHEMA_MATCH_FILE2_PARQUET(
      "large_file2.parquet", 7 * SizeConstants.ONE_MB_IN_BYTES, S3ObjectKind.RANDOM_PARQUET);

  private final String name;
  private final long size;
  private final S3ObjectKind kind;

  private static final long SMALL_BINARY_OBJECTS_LOWER_LIMIT = 8 * SizeConstants.ONE_MB_IN_BYTES;
  private static final long MEDIUM_SIZE_THRESHOLD = 50 * SizeConstants.ONE_MB_IN_BYTES;
  private static final long LARGE_SIZE_THRESHOLD = 500 * SizeConstants.ONE_MB_IN_BYTES;
  private static final List<S3ObjectKind> ENCRYPTED_OBJECT_KINDS =
      Arrays.asList(
          S3ObjectKind.RANDOM_SEQUENTIAL_ENCRYPTED, S3ObjectKind.RANDOM_PARQUET_ENCRYPTED);

  /**
   * Get S3 Object Uri based on the content
   *
   * @param baseUri base URI
   * @return {@link S3URI}
   */
  public S3URI getObjectUri(@NonNull S3URI baseUri) {
    return S3URI.of(
        baseUri.getBucket(),
        baseUri.getKey()
            + "/"
            + this.getKind().getValue().toLowerCase(Locale.getDefault())
            + "/"
            + this.getName());
  }

  /**
   * Returns a filtered list of objects
   *
   * @param predicate predicate that determines whether the object should be returned
   * @return filtered list
   */
  public static List<S3Object> filter(@NonNull Predicate<S3Object> predicate) {
    return Arrays.stream(values())
        .filter(predicate)
        .collect(Collectors.toCollection(ArrayList::new));
  }

  /**
   * Small objects - under 50 MB
   *
   * @return small objects
   */
  public static List<S3Object> smallObjects() {
    return filter(
        o -> o.size < MEDIUM_SIZE_THRESHOLD && !ENCRYPTED_OBJECT_KINDS.contains(o.getKind()));
  }
  /**
   * Returns list of small binary objects (between 8 MB and 50MB, .bin files only).
   *
   * @return list of small binary objects
   */
  public static List<S3Object> smallBinaryObjects() {
    return filter(
        o ->
            o.size >= SMALL_BINARY_OBJECTS_LOWER_LIMIT
                && o.size < MEDIUM_SIZE_THRESHOLD
                && o.getName().endsWith(".bin")
                && !ENCRYPTED_OBJECT_KINDS.contains(o.getKind()));
  }

  /**
   * Medium objects - between 50 MB and 500MB
   *
   * @return medium objects
   */
  public static List<S3Object> mediumObjects() {
    return filter(
        o ->
            o.size >= MEDIUM_SIZE_THRESHOLD
                && o.size < LARGE_SIZE_THRESHOLD
                && !ENCRYPTED_OBJECT_KINDS.contains(o.getKind()));
  }

  /**
   * Small and medium objects - under 500MB
   *
   * @return small and medium objects
   */
  public static List<S3Object> smallAndMediumObjects() {
    return filter(
        o -> o.size < LARGE_SIZE_THRESHOLD && !ENCRYPTED_OBJECT_KINDS.contains(o.getKind()));
  }

  /**
   * Medium and large objects - over 50MB
   *
   * @return medium and large objects
   */
  public static List<S3Object> mediumAndLargeObjects() {
    return filter(
        o -> o.size >= MEDIUM_SIZE_THRESHOLD && !ENCRYPTED_OBJECT_KINDS.contains(o.getKind()));
  }

  /**
   * Large objects - over 500MB
   *
   * @return large objects
   */
  public static List<S3Object> largeObjects() {
    return filter(
        o -> o.size >= LARGE_SIZE_THRESHOLD && !ENCRYPTED_OBJECT_KINDS.contains(o.getKind()));
  }

  /**
   * All objects
   *
   * @return all objects
   */
  public static List<S3Object> allObjects() {
    return filter(o -> true);
  }

  /**
   * Returns list of sequential objects (CSV, JSON, and TXT files).
   *
   * @return list of sequential objects
   */
  public static List<S3Object> getSequentialS3Objects() {
    return filter(
        obj ->
            Arrays.asList("csv", "json", "txt")
                .contains(obj.getName().substring(obj.getName().lastIndexOf('.') + 1)));
  }
}

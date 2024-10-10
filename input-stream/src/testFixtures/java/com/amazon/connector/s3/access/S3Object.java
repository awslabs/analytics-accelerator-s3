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
package com.amazon.connector.s3.access;

import com.amazon.connector.s3.util.S3URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

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
      "random-10gb.bin", 10L * SizeConstants.ONE_GB_IN_BYTES, S3ObjectKind.RANDOM_SEQUENTIAL);

  private final String name;
  private final long size;
  private final S3ObjectKind kind;

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
    return filter(o -> o.size < 50 * SizeConstants.ONE_MB_IN_BYTES);
  }

  /**
   * Medium objects - between 50 MB and 500MB
   *
   * @return small objects
   */
  public static List<S3Object> mediumObjects() {
    return filter(
        o ->
            (o.size >= 50 * SizeConstants.ONE_MB_IN_BYTES)
                && (o.size < 500 * SizeConstants.ONE_MB_IN_BYTES));
  }

  /**
   * Small and medium objects - under 500MB
   *
   * @return small objects
   */
  public static List<S3Object> smallAndMediumObjects() {
    return filter(o -> o.size < 500 * SizeConstants.ONE_MB_IN_BYTES);
  }

  /**
   * Medium and large objects - over 50MB
   *
   * @return small objects
   */
  public static List<S3Object> mediumAndLargeObjects() {
    return filter(o -> o.size >= 50 * SizeConstants.ONE_MB_IN_BYTES);
  }

  /**
   * Medium objects - over 500MB
   *
   * @return small objects
   */
  public static List<S3Object> largeObjects() {
    return filter(o -> o.size >= 500 * SizeConstants.ONE_MB_IN_BYTES);
  }

  /**
   * All objects
   *
   * @return all objects
   */
  public static List<S3Object> allObjects() {
    return filter(o -> true);
  }
}

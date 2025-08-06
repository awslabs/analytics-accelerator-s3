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
package software.amazon.s3.analyticsaccelerator;

import lombok.AllArgsConstructor;
import lombok.Getter;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Attribute;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/** Helper class to streamline Telemetry calls. */
@Getter
@AllArgsConstructor
enum ObjectClientTelemetry {
  URI("uri"),
  RANGE("range"),
  RANGE_LENGTH("range.length");
  private final String name;

  public static final String OPERATION_GET = "s3.client.get";
  public static final String OPERATION_HEAD = "s3.client.head";

  public static final String OPERATION_SYNC_GET = "s3.sync.client.get";
  public static final String OPERATION_SYNC_HEAD = "s3.sync.client.head";

  /**
   * Creates an {@link Attribute} for a {@link S3URI}.
   *
   * @param s3URI the {@link S3URI} to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute uri(S3URI s3URI) {
    return Attribute.of(ObjectClientTelemetry.URI.getName(), s3URI.toString());
  }

  /**
   * Creates an {@link Attribute} for the length of a range.
   *
   * @param range the range
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute range(Range range) {
    return Attribute.of(ObjectClientTelemetry.RANGE.getName(), range.toString());
  }

  /**
   * Creates an {@link Attribute} for the length of a range.
   *
   * @param range the length of the range to measure
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute rangeLength(Range range) {
    return Attribute.of(
        ObjectClientTelemetry.RANGE_LENGTH.getName(), Long.toString(range.getLength()));
  }
}

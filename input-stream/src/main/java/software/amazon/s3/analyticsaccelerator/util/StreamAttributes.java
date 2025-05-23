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
package software.amazon.s3.analyticsaccelerator.util;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import software.amazon.s3.analyticsaccelerator.common.ObjectRange;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Attribute;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.request.Range;

/** Helper class to streamline Telemetry calls. */
@Getter
@AllArgsConstructor
public enum StreamAttributes {
  URI("uri"),
  ETAG("etag"),
  RANGE("range"),
  VARIANT("variant"),
  EFFECTIVE_RANGE("range.effective"),
  GENERATION("generation"),
  COLUMN("column"),
  IOPLAN("ioplan"),
  VECTORED_RANGES("vectored.ranges"),
  LOGICAL_READ_POSITION("logicalread.position"),
  LOGICAL_READ_LENGTH("logicalread.length"),
  RANGE_LENGTH("range.length"),
  STREAM_RELATIVE_TS("stream.relative_ts"),
  LOGICAL_IO_REL_TIMESTAMP("logicalio.ts"),
  PHYSICAL_IO_REL_TIMESTAMP("physicalio.ts");
  private final String name;

  /**
   * Creates an {@link Attribute} for a {@link S3URI}.
   *
   * @param s3URI the {@link S3URI} to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute uri(S3URI s3URI) {
    return Attribute.of(StreamAttributes.URI.getName(), s3URI.toString());
  }

  /**
   * Creates an {@link Attribute} for a etag.
   *
   * @param etag the etag to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute etag(String etag) {
    return Attribute.of(StreamAttributes.ETAG.getName(), etag);
  }

  /**
   * Creates an {@link Attribute} for a {@link S3URI}.
   *
   * @param variant the variant of the operation
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute variant(String variant) {
    return Attribute.of(StreamAttributes.VARIANT.getName(), variant);
  }

  /**
   * Creates an {@link Attribute} for a {@link Range}.
   *
   * @param start range start.
   * @param end range end.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute range(long start, long end) {
    return range(new Range(start, end));
  }

  /**
   * Creates an {@link Attribute} for a {@link Range}.
   *
   * @param range range.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute range(Range range) {
    return Attribute.of(StreamAttributes.RANGE.getName(), range.toString());
  }

  /**
   * Creates an {@link Attribute} for an effective {@link Range}.
   *
   * @param start range start.
   * @param end range end.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute effectiveRange(long start, long end) {
    return effectiveRange(new Range(start, end));
  }

  /**
   * Creates an {@link Attribute} for anm affective {@link Range}.
   *
   * @param range range.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute effectiveRange(Range range) {
    return Attribute.of(StreamAttributes.EFFECTIVE_RANGE.getName(), range.toString());
  }

  /**
   * Creates an {@link Attribute} for generation.
   *
   * @param generation the position to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute generation(long generation) {
    return Attribute.of(StreamAttributes.GENERATION.getName(), generation);
  }

  /**
   * Creates an {@link Attribute} for generation.
   *
   * @param column the position to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute column(String column) {
    return Attribute.of(StreamAttributes.COLUMN.getName(), column);
  }

  /**
   * Creates an {@link Attribute} for ioPlan.
   *
   * @param ioPlan the position to create the attribute from.
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute ioPlan(IOPlan ioPlan) {
    return Attribute.of(StreamAttributes.IOPLAN.getName(), ioPlan.toString());
  }

  /**
   * Creates an {@link Attribute} for vectoredRanges.
   *
   * @param objectRanges the ranges for vectoredReads().
   * @return The new instance of the {@link Attribute}.
   */
  public static Attribute vectoredRanges(List<ObjectRange> objectRanges) {
    return Attribute.of(StreamAttributes.VECTORED_RANGES.getName(), objectRanges.toString());
  }

  /**
   * Creates an {@link Attribute} for recording timestamps since the start of stream.
   *
   * @param ts the timestamp to record
   * @return The new instance of the {@link Attribute}
   */
  public static Attribute streamRelativeTimestamp(long ts) {
    return Attribute.of(StreamAttributes.STREAM_RELATIVE_TS.getName(), ts);
  }

  /**
   * Creates an {@link Attribute} to record the position of reads.
   *
   * @param pos the position to record
   * @return The new instance of the {@link Attribute}
   */
  public static Attribute logicalReadPosition(long pos) {
    return Attribute.of(StreamAttributes.LOGICAL_READ_POSITION.getName(), pos);
  }

  /**
   * Creates an {@link Attribute} to record the length of reads.
   *
   * @param len the length to record
   * @return The new instance of the {@link Attribute}
   */
  public static Attribute logicalReadLength(int len) {
    return Attribute.of(StreamAttributes.LOGICAL_READ_LENGTH.getName(), len);
  }

  /**
   * Creates an {@link Attribute} to record timestamps relative to LogicalIO creation.
   *
   * @param ts the timestamp to record
   * @return The new instance of the {@link Attribute}
   */
  public static Attribute logicalIORelativeTimestamp(long ts) {
    return Attribute.of(StreamAttributes.LOGICAL_IO_REL_TIMESTAMP.getName(), ts);
  }

  /**
   * Creates an {@link Attribute} to record timestamps relative to PhysicalIO creation.
   *
   * @param ts the timestamp to record
   * @return The new instance of the {@link Attribute}
   */
  public static Attribute physicalIORelativeTimestamp(long ts) {
    return Attribute.of(StreamAttributes.PHYSICAL_IO_REL_TIMESTAMP.getName(), ts);
  }

  /**
   * Creates an {@link Attribute} to record timestamps relative to PhysicalIO creation.
   *
   * @param ts the timestamp to record
   * @return The new instance of the {@link Attribute}
   */
  public static Attribute rangeLength(long ts) {
    return Attribute.of(StreamAttributes.RANGE_LENGTH.getName(), ts);
  }
}

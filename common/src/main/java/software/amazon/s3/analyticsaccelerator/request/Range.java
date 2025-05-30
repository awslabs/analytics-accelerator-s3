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
package software.amazon.s3.analyticsaccelerator.request;

import lombok.Value;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.util.RangeType;

/**
 * Object representing a byte range. This class helps us abstract away from S3 SDK constructs and
 * helps with testing. For example, it gets rid of the need of having to generate and parse strings
 * like "bytes=0-555" -- this is SDK detail we should not care about in layers above Object Client.
 */
@Value
public class Range {
  long start;
  long end;
  RangeType rangeType;

  private static final String TO_HTTP_STRING_FORMAT = "bytes=%d-%d";
  private static final String TO_STRING_FORMAT = "%d-%d";

  /**
   * Construct a range. At least one of the start and end of range should be present.
   *
   * @param start the start of the range, possibly empty
   * @param end the end of the range, possibly empty
   */
  public Range(long start, long end) {
    Preconditions.checkArgument(start >= 0, "`start` must not be negative");
    Preconditions.checkArgument(end >= 0, "`end` must not be negative");
    Preconditions.checkArgument(start <= end, "`start` must be less than equal to `end`");

    this.start = start;
    this.end = end;
    this.rangeType = RangeType.BLOCK;
  }

  /**
   * Construct a range. At least one of the start and end of range should be present. Also provides
   * option to specify RangeType.
   *
   * @param start the start of the range, possibly empty
   * @param end the end of the range, possibly empty
   * @param rangeType the section of the parquet file the range refers to.
   */
  public Range(long start, long end, RangeType rangeType) {
    Preconditions.checkArgument(start >= 0, "`start` must not be negative");
    Preconditions.checkArgument(end >= 0, "`end` must not be negative");
    Preconditions.checkArgument(start <= end, "`start` must be less than equal to `end`");

    this.start = start;
    this.end = end;
    this.rangeType = rangeType;
  }

  /**
   * Return the length of the range.
   *
   * @return the length of the range in bytes
   */
  public long getLength() {
    return this.end - this.start + 1;
  }

  /**
   * Returns the textual representation of {@link Range}.
   *
   * @return the textual representation of {@link Range}.
   */
  @Override
  public String toString() {
    return String.format(TO_STRING_FORMAT, start, end);
  }

  /**
   * Returns the string representation of {@link Range} used in S3 requests, as defined by the Http
   * RFC.
   *
   * @return the HTTP RFC compatible representation of {@link Range}.
   */
  public String toHttpString() {
    return String.format(TO_HTTP_STRING_FORMAT, start, end);
  }
}

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

import static java.util.Objects.requireNonNull;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.common.ObjectRange;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;

/**
 * Utils class for vectoredReads, to help with things like range validation. Most of the code in
 * this class is written by @mukundthakur, and taken from
 * /hadoop-common/src/main/java/org/apache/hadoop/fs/VectoredReadUtils.java, (thank you!).
 */
public final class VectoredReadUtils {

  private static final Logger LOG = LoggerFactory.getLogger(VectoredReadUtils.class);

  /**
   * Validate a list of ranges (including overlapping checks) and return the sorted list.
   *
   * <p>Two ranges overlap when the start offset of second is less than the end offset of first. End
   * offset is calculated as start offset + length.
   *
   * @param input input list
   * @param objectLength length if known
   * @return a new sorted list.
   * @throws IllegalArgumentException if there are overlapping ranges or a range element is invalid
   *     (other than with negative offset)
   * @throws EOFException if the last range extends beyond the end of the file supplied or a range
   *     offset is negative
   */
  public static List<ObjectRange> validateAndSortRanges(
      final List<ObjectRange> input, final long objectLength) throws EOFException {

    requireNonNull(input, "Null input list");

    if (input.isEmpty()) {
      // this may seem a pathological case, but it was valid
      // before and somehow Spark can call it through parquet.
      LOG.debug("Empty input list");
      return input;
    }

    final List<ObjectRange> sortedRanges;

    if (input.size() == 1) {
      validateRangeRequest(input.get(0));
      sortedRanges = input;
    } else {
      sortedRanges = sortRangeList(input);
      ObjectRange prev = null;
      for (final ObjectRange current : sortedRanges) {
        validateRangeRequest(current);
        if (prev != null) {
          Preconditions.checkArgument(
              current.getOffset() >= prev.getOffset() + prev.getLength(),
              "Overlapping ranges %s and %s",
              prev,
              current);
        }
        prev = current;
      }
    }
    // at this point the final element in the list is the last range
    // so make sure it is not beyond the end of the file, if passed in.
    // where invalid is: starts at or after the end of the file
    final ObjectRange last = sortedRanges.get(sortedRanges.size() - 1);
    // this check is superfluous, but it allows for different exception message.
    if (last.getOffset() >= objectLength) {
      throw new EOFException("Range starts beyond the file length (" + objectLength + "): " + last);
    }
    if (last.getOffset() + last.getLength() > objectLength) {
      throw new EOFException(
          "Range extends beyond the file length (" + objectLength + "): " + last);
    }

    return sortedRanges;
  }

  /**
   * Validate a single range.
   *
   * @param range range to validate.
   * @return the range.
   * @throws IllegalArgumentException the range length is negative or other invalid condition is met
   *     other than the those which raise EOFException or NullPointerException.
   * @throws EOFException the range offset is negative
   * @throws NullPointerException if the range is null.
   */
  public static ObjectRange validateRangeRequest(ObjectRange range) throws EOFException {

    requireNonNull(range, "range is null");

    Preconditions.checkArgument(range.getLength() >= 0, "length is negative in %s", range);
    if (range.getOffset() < 0) {
      throw new EOFException("position is negative in range " + range);
    }
    return range;
  }

  /**
   * Sort the input ranges by offset; no validation is done.
   *
   * @param input input ranges.
   * @return a new list of the ranges, sorted by offset.
   */
  public static List<ObjectRange> sortRangeList(List<ObjectRange> input) {
    final List<ObjectRange> l = new ArrayList<>(input);
    l.sort(Comparator.comparingLong(ObjectRange::getOffset));
    return l;
  }
}

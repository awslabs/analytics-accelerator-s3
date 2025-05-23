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

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static software.amazon.s3.analyticsaccelerator.util.VectoredReadUtils.*;

import java.io.EOFException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.common.ObjectRange;

/**
 * Test for validating readVectored() ranges. These test are mostly just copied over from
 * hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/fs/impl/TestVectoredReadUtils.java.
 * Authors are @steveloughran and @mukundthakur.
 */
public class VectoredReadUtilsTest {

  @Test
  public void testRejectOverlappingRanges() throws Exception {
    List<ObjectRange> input =
        asList(
            createObjectRange(100, 100), createObjectRange(200, 100), createObjectRange(250, 100));

    assertThrows(IllegalArgumentException.class, () -> validateAndSortRanges(input, 500));
  }

  /** Special case of overlap: the ranges are equal. */
  @Test
  public void testDuplicateRangesRaisesIllegalArgument() throws Exception {

    List<ObjectRange> input =
        asList(
            createObjectRange(100, 100),
            createObjectRange(500, 100),
            createObjectRange(1000, 100),
            createObjectRange(1000, 100));

    assertThrows(IllegalArgumentException.class, () -> validateAndSortRanges(input, 500));
  }

  /** Consecutive ranges MUST pass. */
  @Test
  public void testConsecutiveRangesAreValid() throws Throwable {
    validateAndSortRanges(
        asList(
            createObjectRange(100, 100), createObjectRange(200, 100), createObjectRange(300, 100)),
        800);
  }

  /** Empty ranges are allowed. */
  @Test
  public void testEmptyRangesAllowed() throws Throwable {
    validateAndSortRanges(Collections.emptyList(), 200);
  }

  /** Reject negative offsets. */
  @Test
  public void testNegativeOffsetRaisesEOF() throws Throwable {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            validateAndSortRanges(
                asList(createObjectRange(1000, 100), createObjectRange(-1000, 100)), 200));
  }

  /** Reject negative lengths. */
  @Test
  public void testNegativePositionRaisesIllegalArgument() throws Throwable {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            validateAndSortRanges(
                asList(createObjectRange(1000, 100), createObjectRange(1000, -100)), 100));
  }

  /** A read for a whole file is valid. */
  @Test
  public void testReadWholeFile() throws Exception {
    final int length = 1000;

    // Read whole file as one element
    final List<ObjectRange> ranges =
        validateAndSortRanges(asList(createObjectRange(0, length)), length);

    assertEquals(ranges.size(), 1);
    assertEquals(ranges.get(0).getOffset(), 0);
    assertEquals(ranges.get(0).getLength(), length);
  }

  /** A read from start of file to past EOF is rejected. */
  @Test
  public void testReadPastEOFRejected() throws Exception {
    final int length = 1000;
    assertThrows(
        EOFException.class,
        () -> validateAndSortRanges(asList(createObjectRange(0, length + 1)), length));
  }

  /** If the start offset is at the end of the file: an EOFException. */
  @Test
  public void testReadStartingPastEOFRejected() throws Exception {
    final int length = 1000;
    assertThrows(
        EOFException.class,
        () -> validateAndSortRanges(asList(createObjectRange(length, 0)), length));
  }

  /** A read from just below the EOF to the end of the file is valid. */
  @Test
  public void testReadUpToEOF() throws Exception {
    final int length = 1000;

    final int p = length - 1;

    final List<ObjectRange> ranges = validateAndSortRanges(asList(createObjectRange(p, 1)), length);

    assertEquals(ranges.size(), 1);
    assertEquals(ranges.get(0).getOffset(), p);
    assertEquals(ranges.get(0).getLength(), 1);
  }

  /**
   * A read from just below the EOF to the just past the end of the file is rejected with
   * EOFException.
   */
  @Test
  public void testReadOverEOFRejected() throws Exception {
    final int length = 1000;

    assertThrows(
        EOFException.class,
        () -> validateAndSortRanges(asList(createObjectRange(length - 1, 2)), length));
  }

  /**
   * Verify that {@link VectoredReadUtils#sortRangeList(List)} returns an array matching the list
   * sort ranges.
   */
  @Test
  public void testArraySortRange() throws Throwable {
    List<ObjectRange> input =
        asList(
            createObjectRange(3000, 100),
            createObjectRange(2100, 100),
            createObjectRange(1000, 100));

    final List<ObjectRange> rangeList = sortRangeList(input);

    assertEquals(rangeList.get(0).getOffset(), 1000);
    assertEquals(rangeList.get(1).getOffset(), 2100);
    assertEquals(rangeList.get(2).getOffset(), 3000);
  }

  private ObjectRange createObjectRange(int start, int length) {
    return new ObjectRange(new CompletableFuture<>(), start, length);
  }
}

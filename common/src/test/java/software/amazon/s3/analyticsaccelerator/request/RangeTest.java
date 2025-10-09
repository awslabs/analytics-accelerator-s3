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

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class RangeTest {

  @ParameterizedTest
  @MethodSource("invalidRanges")
  void testInvalidRangesThrow(long start, long end) {
    assertThrows(IllegalArgumentException.class, () -> new Range(start, end));
  }

  @ParameterizedTest
  @MethodSource("validStringRanges")
  void testToString(long start, long end, String expected) {
    assertEquals(expected, new Range(start, end).toString());
  }

  @ParameterizedTest
  @MethodSource("validHttpStringRanges")
  void testToHttpString(long start, long end, String expected) {
    assertEquals(expected, new Range(start, end).toHttpString());
  }

  @Test
  void testSize() {
    assertEquals(1, new Range(0, 0).getLength());
    assertEquals(100, new Range(0, 99).getLength());
  }

  @Test
  void testContains() {
    Range range = new Range(0, 100);
    assertTrue(range.contains(50));
    assertFalse(range.contains(120));
  }

  @Test
  void testCompareTo() {
    Range range1 = new Range(0, 100);
    Range range2 = new Range(100, 200);
    Range range3 = new Range(200, 300);
    assertTrue(range1.compareTo(range2) < 0);
    assertTrue(range2.compareTo(range3) < 0);
    assertTrue(range3.compareTo(range1) > 0);
  }

  static Stream<Arguments> validStringRanges() {
    return Stream.of(
        Arguments.of(1, 5, "1-5"),
        Arguments.of(0, 0, "0-0"),
        Arguments.of(100, Long.MAX_VALUE, "100-" + Long.MAX_VALUE));
  }

  static Stream<Arguments> validHttpStringRanges() {
    return Stream.of(
        Arguments.of(1, 5, "bytes=1-5"),
        Arguments.of(0, 0, "bytes=0-0"),
        Arguments.of(100, Long.MAX_VALUE, "bytes=100-" + Long.MAX_VALUE));
  }

  static Stream<Arguments> invalidRanges() {
    return Stream.of(
        Arguments.of(7, 5), Arguments.of(-100, 5), Arguments.of(1, -100), Arguments.of(-1, 1));
  }
}

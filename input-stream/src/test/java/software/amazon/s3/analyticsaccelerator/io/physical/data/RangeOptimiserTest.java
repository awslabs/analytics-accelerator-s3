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
package software.amazon.s3.analyticsaccelerator.io.physical.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.Range;

public class RangeOptimiserTest {

  private PhysicalIOConfiguration mockConfig;
  private RangeOptimiser rangeOptimiser;
  private static final long READ_BUFFER_SIZE = 1024;
  private static final long MAX_RANGE_SIZE = 4 * READ_BUFFER_SIZE;
  private static final long PART_SIZE = 3 * READ_BUFFER_SIZE;

  @BeforeEach
  void setUp() {
    mockConfig = mock(PhysicalIOConfiguration.class);
    when(mockConfig.getMaxRangeSizeBytes()).thenReturn(MAX_RANGE_SIZE);
    when(mockConfig.getPartSizeBytes()).thenReturn(PART_SIZE);

    rangeOptimiser = new RangeOptimiser(mockConfig);
  }

  @Test
  public void testSplitRanges_noSplitNeeded() {
    Range range = new Range(0, MAX_RANGE_SIZE - 1);
    List<Range> ranges = Collections.singletonList(range);

    List<Range> result = rangeOptimiser.splitRanges(ranges);

    assertEquals(1, result.size(), "Range under max size should not be split");
    assertEquals(range, result.get(0), "Range should remain unchanged");
  }

  @Test
  public void testSplitRanges_splitNeeded() {
    Range range = new Range(0, MAX_RANGE_SIZE + PART_SIZE);
    List<Range> ranges = Collections.singletonList(range);

    List<Range> result = rangeOptimiser.splitRanges(ranges);

    assertEquals(
        3, result.size(), "Range exceeding max size should be split into correct number of parts");
    assertEquals(new Range(0, PART_SIZE - 1), result.get(0), "First part should match part size");
    assertEquals(
        new Range(PART_SIZE, 2 * PART_SIZE - 1),
        result.get(1),
        "Second part should match part size");
    assertEquals(
        new Range(2 * PART_SIZE, MAX_RANGE_SIZE + PART_SIZE),
        result.get(2),
        "Last part should contain remainder");
  }

  @Test
  public void testOptimizeReads_emptyList() {
    List<List<Integer>> result =
        rangeOptimiser.optimizeReads(Collections.emptyList(), READ_BUFFER_SIZE);
    assertTrue(result.isEmpty(), "Result should be empty for empty input");
  }

  @Test
  public void testOptimizeReads_basicSequentialGrouping() {
    // Example 1: Basic sequential grouping
    List<Integer> input = Arrays.asList(1, 2, 3, 5, 6, 8, 9, 10);
    List<List<Integer>> expected =
        Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(5, 6), Arrays.asList(8, 9, 10));

    List<List<Integer>> result = rangeOptimiser.optimizeReads(input, READ_BUFFER_SIZE);

    assertEquals(expected.size(), result.size(), "Should have the same number of groups");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i), "Group " + i + " should match");
    }
  }

  @Test
  public void testOptimizeReads_sizeSplitting() {
    // Example 2: Size-based splitting
    List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    // With maxRangeBlocks=4 and partSizeBlocks=3
    // Expected: [[1,2,3], [4,5,6], [7,8,9], [10]]
    List<List<Integer>> expected =
        Arrays.asList(
            Arrays.asList(1, 2, 3),
            Arrays.asList(4, 5, 6),
            Arrays.asList(7, 8, 9),
            Arrays.asList(10));

    List<List<Integer>> result = rangeOptimiser.optimizeReads(input, READ_BUFFER_SIZE);

    assertEquals(expected.size(), result.size(), "Should have the same number of groups");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i), "Group " + i + " should match");
    }
  }

  @Test
  public void testOptimizeReads_mixedSplitting() {
    // Example 3: Mixed sequential and size-based splitting
    List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 10, 11, 12, 13, 14, 15, 16, 17);

    // With maxRangeBlocks=3 and partSizeBlocks=2
    when(mockConfig.getMaxRangeSizeBytes()).thenReturn(3 * READ_BUFFER_SIZE);
    when(mockConfig.getPartSizeBytes()).thenReturn(2 * READ_BUFFER_SIZE);

    // Expected: [[1,2], [3,4], [5,6], [10,11], [12,13], [14,15], [16,17]]
    List<List<Integer>> expected =
        Arrays.asList(
            Arrays.asList(1, 2),
            Arrays.asList(3, 4),
            Arrays.asList(5, 6),
            Arrays.asList(10, 11),
            Arrays.asList(12, 13),
            Arrays.asList(14, 15),
            Arrays.asList(16, 17));

    List<List<Integer>> result = rangeOptimiser.optimizeReads(input, READ_BUFFER_SIZE);

    assertEquals(expected.size(), result.size(), "Should have the same number of groups");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i), "Group " + i + " should match");
    }
  }

  @Test
  public void testOptimizeReads_singleBlock() {
    List<Integer> input = Collections.singletonList(42);
    List<List<Integer>> expected = Collections.singletonList(Collections.singletonList(42));

    List<List<Integer>> result = rangeOptimiser.optimizeReads(input, READ_BUFFER_SIZE);

    assertEquals(expected, result, "Single block should be in its own group");
  }

  @Test
  public void testOptimizeReads_ensureMinimumBlockSize() {
    // Test when configuration would result in zero blocks per read
    when(mockConfig.getMaxRangeSizeBytes()).thenReturn(READ_BUFFER_SIZE / 2);
    when(mockConfig.getPartSizeBytes()).thenReturn(READ_BUFFER_SIZE / 2);

    List<Integer> input = Arrays.asList(1, 2, 3);
    List<List<Integer>> expected =
        Arrays.asList(
            Collections.singletonList(1),
            Collections.singletonList(2),
            Collections.singletonList(3));

    List<List<Integer>> result = rangeOptimiser.optimizeReads(input, READ_BUFFER_SIZE);

    assertEquals(
        expected.size(),
        result.size(),
        "Should have one group per block when max size is less than block size");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i), "Group " + i + " should match");
    }
  }

  @Test
  public void testOptimizeReads_nullInput() {
    List<List<Integer>> result = rangeOptimiser.optimizeReads(null, READ_BUFFER_SIZE);
    assertTrue(result.isEmpty(), "Result should be empty for null input");
  }

  @Test
  public void testOptimizeReads_nonSequentialLargeGroups() {
    // Test with non-sequential groups that each exceed maxRangeBlocks
    when(mockConfig.getMaxRangeSizeBytes()).thenReturn(2 * READ_BUFFER_SIZE);
    when(mockConfig.getPartSizeBytes()).thenReturn(1 * READ_BUFFER_SIZE);

    // Three non-sequential groups, each exceeding maxRangeBlocks=2
    List<Integer> input = Arrays.asList(1, 2, 3, 5, 6, 7, 10, 11, 12);

    // Expected: Each group split into chunks of partSizeBlocks=1
    List<List<Integer>> expected =
        Arrays.asList(
            Collections.singletonList(1),
            Collections.singletonList(2),
            Collections.singletonList(3),
            Collections.singletonList(5),
            Collections.singletonList(6),
            Collections.singletonList(7),
            Collections.singletonList(10),
            Collections.singletonList(11),
            Collections.singletonList(12));

    List<List<Integer>> result = rangeOptimiser.optimizeReads(input, READ_BUFFER_SIZE);

    assertEquals(expected.size(), result.size(), "Should have correct number of groups");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i), "Group " + i + " should match");
    }
  }

  @Test
  public void testOptimizeReads_mixedGroupSizes() {
    // Test with mixed group sizes - some within maxRangeBlocks, some exceeding
    when(mockConfig.getMaxRangeSizeBytes()).thenReturn(2 * READ_BUFFER_SIZE);
    when(mockConfig.getPartSizeBytes()).thenReturn(1 * READ_BUFFER_SIZE);

    // First group (1,2) within limit, second group (4,5,6) exceeds, third group (8) within limit
    List<Integer> input = Arrays.asList(1, 2, 4, 5, 6, 8);

    // Expected: First and third groups unchanged, second group split
    List<List<Integer>> expected =
        Arrays.asList(
            Arrays.asList(1, 2),
            Collections.singletonList(4),
            Collections.singletonList(5),
            Collections.singletonList(6),
            Collections.singletonList(8));

    List<List<Integer>> result = rangeOptimiser.optimizeReads(input, READ_BUFFER_SIZE);

    assertEquals(expected.size(), result.size(), "Should have correct number of groups");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i), "Group " + i + " should match");
    }
  }
}

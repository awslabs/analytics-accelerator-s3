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

public class RangeOptimiserTest {

  private PhysicalIOConfiguration mockConfig;
  private RangeOptimiser rangeOptimiser;
  private static final long READ_BUFFER_SIZE = 1024;
  private static final long TARGET_REQUEST_SIZE = 3 * READ_BUFFER_SIZE;
  private static final double REQUEST_TOLERANCE_RATIO = 1.4;

  @BeforeEach
  void setUp() {
    mockConfig = mock(PhysicalIOConfiguration.class);
    when(mockConfig.getTargetRequestSize()).thenReturn(TARGET_REQUEST_SIZE);
    when(mockConfig.getRequestToleranceRatio()).thenReturn(REQUEST_TOLERANCE_RATIO);
    when(mockConfig.getReadBufferSize()).thenReturn(READ_BUFFER_SIZE);

    rangeOptimiser = new RangeOptimiser(mockConfig);
  }

  @Test
  public void testOptimizeReads_emptyList() {
    List<List<Integer>> result = rangeOptimiser.optimizeReads(Collections.emptyList());
    assertTrue(result.isEmpty(), "Result should be empty for empty input");
  }

  @Test
  public void testOptimizeReads_nullInput() {
    List<List<Integer>> result = rangeOptimiser.optimizeReads(null);
    assertTrue(result.isEmpty(), "Result should be empty for null input");
  }

  @Test
  public void testOptimizeReads_basicSequentialGrouping() {
    // Example 1: Basic sequential grouping
    List<Integer> input = Arrays.asList(1, 2, 3, 5, 6, 8, 9, 10);
    List<List<Integer>> expected =
        Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(5, 6), Arrays.asList(8, 9, 10));

    List<List<Integer>> result = rangeOptimiser.optimizeReads(input);

    assertEquals(expected.size(), result.size(), "Should have the same number of groups");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i), "Group " + i + " should match");
    }
  }

  @Test
  public void testOptimizeReads_sizeSplitting() {
    // Example 2: Size-based splitting
    List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    // With targetBlocks=3 and threshold=1.4 (splitThreshold=4.2, so 5+ blocks get split)
    // 10 blocks > 4.2 threshold, so split into chunks of 3 since the last chunk has 4 elements
    // which is less than 4.2 we are not splitting it
    // Expected: [[1,2,3], [4,5,6], [7,8,9,10]]
    List<List<Integer>> expected =
        Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6), Arrays.asList(7, 8, 9, 10));

    List<List<Integer>> result = rangeOptimiser.optimizeReads(input);

    assertEquals(expected.size(), result.size(), "Should have the same number of groups");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i), "Group " + i + " should match");
    }
  }

  @Test
  public void testOptimizeReads_mixedSplitting() {
    // Example 3: Mixed sequential and size-based splitting
    List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 10, 11, 12, 13, 14, 15, 16, 17);

    // With targetBlocks=2 and threshold=1.4 (splitThreshold=2.8, so 3+ blocks get split)
    when(mockConfig.getTargetRequestSize()).thenReturn(2 * READ_BUFFER_SIZE);
    when(mockConfig.getRequestToleranceRatio()).thenReturn(1.4);

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

    List<List<Integer>> result = rangeOptimiser.optimizeReads(input);

    assertEquals(expected.size(), result.size(), "Should have the same number of groups");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i), "Group " + i + " should match");
    }
  }

  @Test
  public void testOptimizeReads_singleBlock() {
    List<Integer> input = Collections.singletonList(42);
    List<List<Integer>> expected = Collections.singletonList(Collections.singletonList(42));

    List<List<Integer>> result = rangeOptimiser.optimizeReads(input);

    assertEquals(expected, result, "Single block should be in its own group");
  }

  @Test
  public void testOptimizeReads_nonSequentialLargeGroups() {
    // Test with non-sequential groups that each exceed threshold
    when(mockConfig.getTargetRequestSize()).thenReturn(READ_BUFFER_SIZE);
    when(mockConfig.getRequestToleranceRatio()).thenReturn(1.4);

    // Three non-sequential groups, each with 3 blocks > threshold of 1.4
    List<Integer> input = Arrays.asList(1, 2, 3, 5, 6, 7, 10, 11, 12);

    // Expected: Each group split into chunks of targetBlocks=1
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

    List<List<Integer>> result = rangeOptimiser.optimizeReads(input);

    assertEquals(expected.size(), result.size(), "Should have correct number of groups");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i), "Group " + i + " should match");
    }
  }

  @Test
  public void testOptimizeReads_mixedGroupSizes() {
    // Test with mixed group sizes - some within threshold, some exceeding
    when(mockConfig.getTargetRequestSize()).thenReturn(2 * READ_BUFFER_SIZE);
    when(mockConfig.getRequestToleranceRatio()).thenReturn(1.4);

    // First group (1,2) within threshold of 3 (rounded from 2.8), second group (4,5,6) equals
    // threshold, third group (8)
    // within threshold
    List<Integer> input = Arrays.asList(1, 2, 4, 5, 6, 8);

    // Expected: All groups unchanged since (4,5,6) has 3 blocks which equals threshold of 3
    List<List<Integer>> expected =
        Arrays.asList(Arrays.asList(1, 2), Arrays.asList(4, 5, 6), Collections.singletonList(8));

    List<List<Integer>> result = rangeOptimiser.optimizeReads(input);

    assertEquals(expected.size(), result.size(), "Should have correct number of groups");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i), "Group " + i + " should match");
    }
  }

  @Test
  public void testOptimizeReads_remainderMerging() {
    // Test remainder merging with small tolerance
    when(mockConfig.getTargetRequestSize()).thenReturn(3 * READ_BUFFER_SIZE);
    when(mockConfig.getRequestToleranceRatio()).thenReturn(1.1);

    // Group of 7 blocks: targetBlocks=3, threshold=3 (rounded from 3.3), so 7 > 3 → split
    // Normal split would be [1,2,3], [4,5,6], [7]
    // But [4,5,6] + [7] = 4 blocks > 3 threshold, so don't merge
    List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 7);

    // Expected: [1,2,3], [4,5,6], [7] (remainder not merged due to rounding)
    List<List<Integer>> expected =
        Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6), Collections.singletonList(7));

    List<List<Integer>> result = rangeOptimiser.optimizeReads(input);

    assertEquals(expected.size(), result.size(), "Should have correct number of groups");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i), "Group " + i + " should match");
    }
  }

  @Test
  public void testOptimizeReads_remainderTooLargeToMerge() {
    // Test when remainder is too large to merge
    when(mockConfig.getTargetRequestSize()).thenReturn(3 * READ_BUFFER_SIZE);
    when(mockConfig.getRequestToleranceRatio()).thenReturn(1.1);

    // Group of 8 blocks: targetBlocks=3, threshold=3.3
    // Normal split would be [1,2,3], [4,5,6], [7,8]
    // [4,5,6] + [7,8] = 5 blocks > 3.3 threshold, so don't merge
    List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8);

    // Expected: [1,2,3], [4,5,6], [7,8] (remainder not merged)
    List<List<Integer>> expected =
        Arrays.asList(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6), Arrays.asList(7, 8));

    List<List<Integer>> result = rangeOptimiser.optimizeReads(input);

    assertEquals(expected.size(), result.size(), "Should have correct number of groups");
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i), "Group " + i + " should match");
    }
  }
}

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
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.Range;

public class RangeOptimiserTest {

  @Test
  public void test__splitRanges__smallRangesCauseNoSplit() {
    // Given: small ranges
    RangeOptimiser rangeOptimiser = new RangeOptimiser(PhysicalIOConfiguration.DEFAULT);
    List<Range> ranges = new LinkedList<>();
    ranges.add(new Range(0, 100));
    ranges.add(new Range(200, 300));
    ranges.add(new Range(300, 400));
    ranges.add(new Range(400, 500));

    // When: splitRanges is called
    List<Range> splitRanges = rangeOptimiser.splitRanges(ranges);

    // Then: nothing happens
    assertEquals(ranges, splitRanges);
  }

  @Test
  public void test__splitRanges__bigRangesResultInSplits() {
    // Given: a 16MB range
    RangeOptimiser rangeOptimiser = new RangeOptimiser(PhysicalIOConfiguration.DEFAULT);
    List<Range> ranges = new LinkedList<>();
    ranges.add(new Range(0, 16 * ONE_MB - 1));

    // When: splitRanges is called
    List<Range> splitRanges = rangeOptimiser.splitRanges(ranges);

    // Then: 16MB range is split into 4x4MB ranges
    List<Range> expected = new LinkedList<>();
    expected.add(new Range(0, 8 * ONE_MB - 1));
    expected.add(new Range(8 * ONE_MB, 16 * ONE_MB - 1));
    assertEquals(expected, splitRanges);
  }

  @Test
  public void test__splitRanges__combinesSmallRangesAtEnd() {
    // Given: a custom configuration with smaller part size to test combining small ranges
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder()
            .partSizeBytes(8 * ONE_MB)
            .maxRangeSizeBytes(16 * ONE_MB)
            .build();
    RangeOptimiser rangeOptimiser = new RangeOptimiser(config);

    // Create a range that will split into 3 parts, with the last part being small
    List<Range> ranges = new LinkedList<>();
    ranges.add(new Range(0, 19 * ONE_MB - 1)); // 19MB range

    // When: splitRanges is called
    List<Range> splitRanges = rangeOptimiser.splitRanges(ranges);

    // Then: The range should be split into 2 parts, with the last 2 parts combined
    // First part: 0 to 8MB-1
    // Second part: 8MB to 19MB-1 (combined the original 8MB-16MB-1 and 16MB-19MB-1)
    List<Range> expected = new LinkedList<>();
    expected.add(new Range(0, 8 * ONE_MB - 1));
    expected.add(new Range(8 * ONE_MB, 19 * ONE_MB - 1));

    assertEquals(expected, splitRanges);
  }

  @Test
  public void test__splitRanges__exactlyMatchingScenario() {
    // Given: a custom configuration to match the specific scenario in the bug report
    PhysicalIOConfiguration config =
        PhysicalIOConfiguration.builder()
            .partSizeBytes(16 * ONE_MB)
            .maxRangeSizeBytes(32 * ONE_MB)
            .build();
    RangeOptimiser rangeOptimiser = new RangeOptimiser(config);

    // Create a range that will split into parts like [16, 32] and [33, 35]
    List<Range> ranges = new LinkedList<>();
    ranges.add(new Range(16, 35)); // Range from 16 to 35

    // When: splitRanges is called
    List<Range> splitRanges = rangeOptimiser.splitRanges(ranges);

    // Then: The range should not be split since it's smaller than maxRangeSizeBytes
    List<Range> expected = new LinkedList<>();
    expected.add(new Range(16, 35));

    assertEquals(expected, splitRanges);

    // Now test with a larger range that would create the problematic split
    ranges.clear();
    ranges.add(new Range(0, 35 * ONE_MB)); // Large range that will be split

    // When: splitRanges is called
    splitRanges = rangeOptimiser.splitRanges(ranges);

    // Then: The last part should be combined with the previous part if it's small
    assertEquals(2, splitRanges.size()); // Should have 2 parts, not 3
    assertEquals(0, splitRanges.get(0).getStart());
    assertEquals(16 * ONE_MB - 1, splitRanges.get(0).getEnd());
    assertEquals(16 * ONE_MB, splitRanges.get(1).getStart());
    assertEquals(35 * ONE_MB, splitRanges.get(1).getEnd());
  }
}

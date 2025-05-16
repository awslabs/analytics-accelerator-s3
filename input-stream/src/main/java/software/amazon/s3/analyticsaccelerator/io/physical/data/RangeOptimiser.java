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

import java.util.LinkedList;
import java.util.List;
import lombok.Value;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.util.RangeType;

/**
 * RangeSplitter is responsible for splitting up big ranges into smaller reads. The need for such
 * functionality arises from sequential prefetching. When we decide that, e.g., the next 128MB chunk
 * of an object is needed with high confidence, then we should not fetch this in a single request.
 *
 * <p>This class is capable of implementing heuristics on how to fetch ranges of different sizes
 * optimally.
 */
@Value
public class RangeOptimiser {
  PhysicalIOConfiguration configuration;

  /**
   * Given a list of ranges, return a potentially new set of ranges which is more optimal to fetch
   * (i.e., split up huge ranges based on a heuristic).
   *
   * @param ranges a list of ranges
   * @return a potentially different list of ranges with big ranges split up
   */
  public List<Range> splitRanges(List<Range> ranges) {
    List<Range> splits = new LinkedList<>();
    for (Range range : ranges) {
      if (range.getLength() > configuration.getMaxRangeSizeBytes()) {
        splitRange(range.getStart(), range.getEnd(), range.getRangeType()).forEach(splits::add);
      } else {
        splits.add(range);
      }
    }

    return splits;
  }

  private List<Range> splitRange(long start, long end, RangeType rangeType) {
    long nextRangeStart = start;
    List<Range> generatedRanges = new LinkedList<>();

    while (nextRangeStart < end) {
      long rangeEnd = Math.min(nextRangeStart + configuration.getPartSizeBytes() - 1, end);
      generatedRanges.add(new Range(nextRangeStart, rangeEnd, rangeType));
      nextRangeStart = rangeEnd + 1;
    }

    return generatedRanges;
  }
}

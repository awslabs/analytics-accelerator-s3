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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.Range;

/**
 * Utility class for object size-related operations and determinations. Provides methods to classify
 * objects based on their size and configuration settings.
 */
public class AnalyticsAcceleratorUtils {
  /**
   * Determines if an object should be treated as a small object based on the given configuration.
   *
   * @param configuration The PhysicalIOConfiguration containing small object settings
   * @param contentLength The length of the object in bytes
   * @return true if the object should be treated as a small object
   */
  public static boolean isSmallObject(PhysicalIOConfiguration configuration, long contentLength) {
    return configuration.isSmallObjectsPrefetchingEnabled()
        && contentLength <= configuration.getSmallObjectSizeThreshold();
  }

  public static List<Range> coalesceRanges(List<Range> currentRanges, long tolerance) {

    List<Range> coalescedRages = new ArrayList<>();

    if (currentRanges.size() < 2) {
      return currentRanges;
    }

    // Ensure ranges are ordered by their start position.
    Collections.sort(currentRanges);
    Range currentRange = currentRanges.get(0);
    for (int i = 1; i < currentRanges.size(); i++) {
      Range nextRange = currentRanges.get(i);

      if (currentRange.getEnd() + tolerance >= nextRange.getStart()) {
        currentRange =
            new Range(currentRange.getStart(), Math.max(currentRange.getEnd(), nextRange.getEnd()));
      } else {
        coalescedRages.add(currentRange);
        currentRange = nextRange;
      }
    }

    return coalescedRages;
  }
}

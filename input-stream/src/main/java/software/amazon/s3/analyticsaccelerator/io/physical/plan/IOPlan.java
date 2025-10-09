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
package software.amazon.s3.analyticsaccelerator.io.physical.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.request.Range;

/** A logical IO plan */
@Getter
public class IOPlan {
  private final ArrayList<Range> prefetchRanges;
  public static final IOPlan EMPTY_PLAN = new IOPlan(Collections.emptyList());

  /**
   * Creates a new instance of {@link IOPlan}
   *
   * @param prefetchRange single prefetch range
   */
  public IOPlan(@NonNull Range prefetchRange) {
    this.prefetchRanges = new ArrayList<>(1);
    this.prefetchRanges.add(prefetchRange);
  }
  /**
   * Creates a new instance of {@link IOPlan}
   *
   * @param prefetchRanges prefetch ranges
   */
  public IOPlan(@NonNull Collection<Range> prefetchRanges) {
    this.prefetchRanges = new ArrayList<>(prefetchRanges);
  }

  /**
   * Returns a string representation of the object.
   *
   * @return a string representation of the object.
   */
  @Override
  public String toString() {
    return "["
        + this.prefetchRanges.stream().map(Range::toString).collect(Collectors.joining(","))
        + "]";
  }

  /**
   * Coalesces the prefetch ranges in this plan. The coalescing is done in-place.
   *
   * @param tolerance the tolerance for coalescing
   */
  public void coalesce(long tolerance) {
    if (this.prefetchRanges.size() < 2) {
      return;
    }
    // Ensure ranges are ordered by their start position.
    Collections.sort(this.prefetchRanges);
    int writeIndex = 0;
    Range currentRange = this.prefetchRanges.get(0);
    for (int i = 1; i < this.prefetchRanges.size(); i++) {
      Range nextRange = this.prefetchRanges.get(i);
      // Always compare with the last checked interval to avoid n^2 comparisons
      if (currentRange.getEnd() + tolerance >= nextRange.getStart()) {
        currentRange =
            new Range(currentRange.getStart(), Math.max(currentRange.getEnd(), nextRange.getEnd()));
      } else {
        this.prefetchRanges.set(writeIndex++, currentRange);
        currentRange = nextRange;
      }
    }
    this.prefetchRanges.set(writeIndex++, currentRange);
    // writeIndex now shows how many coalesced ranges added to the list.
    // We should remove residue ones from the original list.
    while (this.prefetchRanges.size() > writeIndex) {
      this.prefetchRanges.remove(this.prefetchRanges.size() - 1);
    }
  }
}

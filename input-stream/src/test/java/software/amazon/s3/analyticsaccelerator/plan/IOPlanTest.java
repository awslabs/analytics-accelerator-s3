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
package software.amazon.s3.analyticsaccelerator.plan;

import static org.junit.jupiter.api.Assertions.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collection;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.request.Range;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class IOPlanTest {
  @Test
  void testRangeListConstructor() {
    ArrayList<Range> ranges = new ArrayList<>();
    ranges.add(new Range(1, 2));
    ranges.add(new Range(10, 20));
    IOPlan ioPlan = new IOPlan(ranges);
    assertArrayEquals(ioPlan.getPrefetchRanges().toArray(), ranges.toArray());
  }

  @Test
  void testRangeConstructor() {
    IOPlan ioPlan = new IOPlan(new Range(1, 2));
    assertArrayEquals(ioPlan.getPrefetchRanges().toArray(), new Range[] {new Range(1, 2)});
  }

  @Test
  void testConstructorThrowOnNulls() {
    assertThrows(NullPointerException.class, () -> new IOPlan((Collection<Range>) null));
    assertThrows(NullPointerException.class, () -> new IOPlan((Range) null));
  }

  @Test
  void testRangeListToString() {
    ArrayList<Range> ranges = new ArrayList<>();
    ranges.add(new Range(1, 2));
    ranges.add(new Range(10, 20));
    IOPlan ioPlan = new IOPlan(ranges);
    assertEquals("[1-2,10-20]", ioPlan.toString());
  }

  @Test
  void testRangeToString() {
    IOPlan ioPlan = new IOPlan(new Range(1, 2));
    assertEquals("[1-2]", ioPlan.toString());
  }

  @Test
  void testEmptyPlanToString() {
    assertEquals("[]", IOPlan.EMPTY_PLAN.toString());
  }

  @Test
  void testCoalesceOverlappingRanges() {
    ArrayList<Range> ranges = new ArrayList<>();
    ranges.add(new Range(1, 5));
    ranges.add(new Range(3, 8));
    IOPlan ioPlan = new IOPlan(ranges);
    ioPlan.coalesce(0);
    assertEquals(1, ioPlan.getPrefetchRanges().size());
    assertEquals(new Range(1, 8), ioPlan.getPrefetchRanges().get(0));
  }

  @Test
  void testCoalesceAdjacentRanges() {
    ArrayList<Range> ranges = new ArrayList<>();
    ranges.add(new Range(1, 5));
    ranges.add(new Range(5, 10));
    IOPlan ioPlan = new IOPlan(ranges);
    ioPlan.coalesce(0);
    assertEquals(1, ioPlan.getPrefetchRanges().size());
    assertEquals(new Range(1, 10), ioPlan.getPrefetchRanges().get(0));
  }

  @Test
  void testCoalesceWithTolerance() {
    ArrayList<Range> ranges = new ArrayList<>();
    ranges.add(new Range(1, 5));
    ranges.add(new Range(7, 12));
    IOPlan ioPlan = new IOPlan(ranges);
    ioPlan.coalesce(2);
    assertEquals(1, ioPlan.getPrefetchRanges().size());
    assertEquals(new Range(1, 12), ioPlan.getPrefetchRanges().get(0));
  }

  @Test
  void testCoalesceNoMerge() {
    ArrayList<Range> ranges = new ArrayList<>();
    ranges.add(new Range(1, 5));
    ranges.add(new Range(10, 15));
    IOPlan ioPlan = new IOPlan(ranges);
    ioPlan.coalesce(0);
    assertEquals(2, ioPlan.getPrefetchRanges().size());
  }

  @Test
  void testCoalesceSingleRange() {
    IOPlan ioPlan = new IOPlan(new Range(1, 5));
    ioPlan.coalesce(0);
    assertEquals(1, ioPlan.getPrefetchRanges().size());
    assertEquals(new Range(1, 5), ioPlan.getPrefetchRanges().get(0));
  }

  @Test
  void testCoalesceEmptyPlan() {
    IOPlan ioPlan = new IOPlan(new ArrayList<>());
    ioPlan.coalesce(0);
    assertEquals(0, ioPlan.getPrefetchRanges().size());
  }

  @Test
  void testCoalesceMultipleRanges() {
    ArrayList<Range> ranges = new ArrayList<>();
    ranges.add(new Range(1, 3));
    ranges.add(new Range(4, 7));
    ranges.add(new Range(9, 11));
    ranges.add(new Range(12, 15));
    IOPlan ioPlan = new IOPlan(ranges);
    ioPlan.coalesce(1);
    assertEquals(2, ioPlan.getPrefetchRanges().size());
    assertEquals(new Range(1, 7), ioPlan.getPrefetchRanges().get(0));
    assertEquals(new Range(9, 15), ioPlan.getPrefetchRanges().get(1));
  }

  @Test
  void testCoalesceUnorderedRanges() {
    ArrayList<Range> ranges = new ArrayList<>();
    ranges.add(new Range(10, 15));
    ranges.add(new Range(1, 5));
    ranges.add(new Range(6, 8));
    IOPlan ioPlan = new IOPlan(ranges);
    ioPlan.coalesce(1);
    assertEquals(2, ioPlan.getPrefetchRanges().size());
    assertEquals(new Range(1, 8), ioPlan.getPrefetchRanges().get(0));
    assertEquals(new Range(10, 15), ioPlan.getPrefetchRanges().get(1));
  }
}

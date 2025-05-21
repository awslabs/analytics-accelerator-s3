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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.OptionalLong;
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.util.RangeType;

/**
 * Class responsible for implementing how to plan reads over a BlockStore. Today its main
 * responsibility is ensuring that there are no overlapping ranges in the BlockStore.
 */
public class IOPlanner {
  private final BlockStore blockStore;

  /**
   * Creates a new instance of {@link IOPlanner}.
   *
   * @param blockStore the {@link BlobStore} to use
   */
  public IOPlanner(@NonNull BlockStore blockStore) {
    this.blockStore = blockStore;
  }

  /**
   * Given the start and end of a range, return which ranges to fetch from the object store to have
   * coverage over the whole range.
   *
   * @param pos the starting position of a read
   * @param end the end of a read
   * @param lastObjectByte the zero-indexed position of the last object byte
   * @return a list of Ranges that need to be fetched
   * @throws IOException if an I/O error occurs
   */
  public List<Range> planRead(long pos, long end, RangeType rangeType, long lastObjectByte)
      throws IOException {
    Preconditions.checkArgument(0 <= pos, "`pos` must be non-negative");
    Preconditions.checkArgument(pos <= end, "`pos` must be less than or equal to `end`");
    Preconditions.checkArgument(
        pos <= lastObjectByte, "`pos` must be less than or equal to `lastObjectByte`");

    List<Range> missingRanges = new LinkedList<>();

    OptionalLong nextMissingByte = blockStore.findNextMissingByte(pos);

    while (nextMissingByte.isPresent()
        && nextMissingByte.getAsLong() <= Math.min(end, lastObjectByte)) {
      OptionalLong nextAvailableByte = blockStore.findNextLoadedByte(nextMissingByte.getAsLong());

      final long endOfRange;
      if (nextAvailableByte.isPresent()) {
        endOfRange = Math.min(end, nextAvailableByte.getAsLong() - 1);
      } else {
        endOfRange = Math.min(end, lastObjectByte);
      }

      missingRanges.add(new Range(nextMissingByte.getAsLong(), endOfRange, rangeType));
      nextMissingByte = blockStore.findNextMissingByte(endOfRange + 1);
    }
    return missingRanges;
  }
}

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

import java.util.ArrayList;
import java.util.List;
import lombok.Value;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;

/**
 * Optimizes read operations by grouping sequential block indexes and splitting large groups into
 * smaller chunks based on configured target request size and tolerance ratio.
 *
 * <p>This class prevents inefficient large requests by splitting oversized sequential groups while
 * merging small remainders to avoid creating too many tiny requests.
 */
@Value
public class RangeOptimiser {
  PhysicalIOConfiguration configuration;

  /**
   * Optimizes read operations by grouping sequential block indexes and splitting large groups.
   *
   * <p>Process:
   *
   * <ol>
   *   <li>Groups consecutive block indexes into sequences
   *   <li>Splits groups exceeding the maximum threshold into target-sized chunks
   *   <li>Merges small remainder chunks when possible to avoid inefficient tiny requests
   * </ol>
   *
   * <p>Example with 3 blocks per target and 1.4 tolerance ratio (max 4 blocks before split):
   *
   * <ul>
   *   <li>Input: [1,2,3,4,5,6,7] → Groups: [[1,2,3,4,5,6,7]]
   *   <li>7 > 4 threshold → Split: [[1,2,3], [4,5,6], [7]]
   *   <li>Merge if [4,5,6] + [7] ≤ 4 → Result: [[1,2,3], [4,5,6,7]]
   * </ul>
   *
   * @param blockIndexes ordered list of block indexes to optimize
   * @return optimized groups of sequential block indexes within size limits
   */
  public List<List<Integer>> optimizeReads(List<Integer> blockIndexes) {
    if (blockIndexes == null || blockIndexes.isEmpty()) {
      return new ArrayList<>();
    }

    int blocksPerTargetRequest = calculateBlocksPerTargetRequest();
    int maxBlocksBeforeSplit = calculateMaxBlocksBeforeSplit(blocksPerTargetRequest);

    List<List<Integer>> sequentialGroups = groupSequentialBlocks(blockIndexes);
    return splitLargeGroups(sequentialGroups, maxBlocksBeforeSplit, blocksPerTargetRequest);
  }

  /**
   * Calculates how many blocks fit within the configured target request size.
   *
   * @return number of blocks per target request (minimum 1)
   */
  private int calculateBlocksPerTargetRequest() {
    return Math.max(
        1, (int) (configuration.getTargetRequestSize() / configuration.getReadBufferSize()));
  }

  /**
   * Calculates the maximum number of blocks allowed in a group before splitting is required. Uses
   * rounding to convert the tolerance ratio calculation to the nearest integer.
   *
   * @param blocksPerTargetRequest number of blocks that fit in target request size
   * @return maximum blocks allowed before splitting (rounded from tolerance calculation)
   */
  private int calculateMaxBlocksBeforeSplit(int blocksPerTargetRequest) {
    return (int) Math.round(blocksPerTargetRequest * configuration.getRequestToleranceRatio());
  }

  /**
   * Groups consecutive block indexes into sequential lists.
   *
   * <p>Example: [1,2,3,5,6,8,9,10] → [[1,2,3], [5,6], [8,9,10]]
   *
   * @param blockIndexes ordered list of block indexes
   * @return list of sequential groups where each group contains consecutive indexes
   */
  private List<List<Integer>> groupSequentialBlocks(List<Integer> blockIndexes) {
    List<List<Integer>> sequentialGroups = new ArrayList<>();
    List<Integer> currentSequence = new ArrayList<>();
    currentSequence.add(blockIndexes.get(0));

    for (int i = 1; i < blockIndexes.size(); i++) {
      int current = blockIndexes.get(i);
      int previous = blockIndexes.get(i - 1);

      if (current == previous + 1) {
        // Continue current sequence
        currentSequence.add(current);
      } else {
        // Start new sequence
        sequentialGroups.add(currentSequence);
        currentSequence = new ArrayList<>();
        currentSequence.add(current);
      }
    }

    // Add final sequence
    if (!currentSequence.isEmpty()) {
      sequentialGroups.add(currentSequence);
    }

    return sequentialGroups;
  }

  /**
   * Splits groups that exceed the maximum threshold into smaller target-sized chunks.
   *
   * <p>Groups within the threshold are left unchanged. Oversized groups are split into chunks of
   * the target size.
   *
   * @param sequentialGroups list of sequential block groups to evaluate
   * @param maxBlocksBeforeSplit maximum blocks allowed before splitting is required
   * @param blocksPerTargetRequest target chunk size when splitting large groups
   * @return list of groups where all groups are within acceptable size limits
   */
  private List<List<Integer>> splitLargeGroups(
      List<List<Integer>> sequentialGroups, int maxBlocksBeforeSplit, int blocksPerTargetRequest) {
    List<List<Integer>> result = new ArrayList<>();

    for (List<Integer> group : sequentialGroups) {
      if (group.size() <= maxBlocksBeforeSplit) {
        // Group is within threshold, don't split
        result.add(group);
      } else {
        // Split oversized group into target-sized chunks
        result.addAll(splitGroupIntoChunks(group, blocksPerTargetRequest));
      }
    }

    return result;
  }

  /**
   * Splits a large group into target-sized chunks with intelligent remainder handling.
   *
   * <p>Creates chunks of the target size, but attempts to merge small final chunks with the
   * previous chunk if the combined size stays within the tolerance threshold. This prevents
   * creating inefficiently small requests.
   *
   * <p>Example with target=3, tolerance=4:
   *
   * <ul>
   *   <li>Input: [1,2,3,4,5,6,7] → Normal split: [[1,2,3], [4,5,6], [7]]
   *   <li>Check merge: [4,5,6] + [7] = 4 ≤ 4 threshold → Merge
   *   <li>Result: [[1,2,3], [4,5,6,7]]
   * </ul>
   *
   * @param group list of block indexes to split into smaller chunks
   * @param blocksPerTargetRequest target size for each chunk
   * @return list of optimally-sized chunks with merged remainders when beneficial
   */
  private List<List<Integer>> splitGroupIntoChunks(
      List<Integer> group, int blocksPerTargetRequest) {
    int maxBlocksBeforeSplit = calculateMaxBlocksBeforeSplit(blocksPerTargetRequest);
    List<List<Integer>> chunks = createInitialChunks(group, blocksPerTargetRequest);
    mergeSmallFinalChunk(chunks, maxBlocksBeforeSplit);
    return chunks;
  }

  private List<List<Integer>> createInitialChunks(List<Integer> group, int blocksPerTargetRequest) {
    List<List<Integer>> chunks = new ArrayList<>(group.size() / blocksPerTargetRequest + 1);
    for (int i = 0; i < group.size(); i += blocksPerTargetRequest) {
      int endIndex = Math.min(i + blocksPerTargetRequest, group.size());
      chunks.add(new ArrayList<>(group.subList(i, endIndex)));
    }
    return chunks;
  }

  private void mergeSmallFinalChunk(List<List<Integer>> chunks, int maxBlocksBeforeSplit) {
    if (chunks.size() < 2) return;

    List<Integer> lastChunk = chunks.get(chunks.size() - 1);
    List<Integer> previousChunk = chunks.get(chunks.size() - 2);

    if (lastChunk.size() < previousChunk.size()
        && (lastChunk.size() + previousChunk.size()) <= maxBlocksBeforeSplit) {
      previousChunk.addAll(lastChunk);
      chunks.remove(chunks.size() - 1);
    }
  }
}

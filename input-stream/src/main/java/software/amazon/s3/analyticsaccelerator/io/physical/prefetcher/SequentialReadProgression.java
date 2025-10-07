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
package software.amazon.s3.analyticsaccelerator.io.physical.prefetcher;

import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

import lombok.AllArgsConstructor;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;

/**
 * Class that implements a mathematical function telling us the size of blocks we should prefetch in
 * a sequential read.
 */
@AllArgsConstructor
public class SequentialReadProgression {

  private final PhysicalIOConfiguration configuration;

  /**
   * Given a generation, returns the size of a sequential prefetch block for that generation. This
   * function is effectively a geometric series today but can be fine-tuned later.
   *
   * @param generation zero-indexed integer representing the generation of a read
   * @return a block size in bytes
   */
  public long getSizeForGeneration(long generation) {
    Preconditions.checkArgument(0 <= generation, "`generation` must be non-negative");

    // 4, 8, 16, 32
    return Math.min(
        2
            * ONE_MB
            * (long)
                Math.pow(
                    configuration.getSequentialPrefetchBase(),
                    Math.floor(configuration.getSequentialPrefetchSpeed() * generation)),
        configuration.getSequentialPrefetchMaxSize());
  }

  /**
   * Returns the maximum generation where the geometric progression reaches the configured maximum
   * size.
   *
   * <p>The formula calculates the inverse of getSizeForGeneration to find when: 2MB * base^(speed *
   * generation) = maxSize
   *
   * <p>Solving for generation: base^(speed * generation) = maxSize / (2MB) speed * generation =
   * log(maxSize / (2MB)) / log(base) generation = log(maxSize / (2MB)) / (log(base) * speed)
   *
   * <p>We add 1 because getSizeForGeneration caps values at maxSize, making the next generation
   * still useful for prefetching. Examples: - For 128MB max: gen 6 = 128MB, gen 7 = 128MB capped -
   * For 127MB max: gen 5 = 64MB, gen 6 = 127MB capped
   *
   * @return the highest generation number before the size would exceed the maximum prefetch size
   */
  public int getMaximumGeneration() {
    // Add 1 because getSizeForGeneration caps at max size, so the next generation
    // is still useful for prefetching (e.g., for 127MB max: gen 5 = 64MB, gen 6 = 127MB capped)
    return (int)
            Math.floor(
                Math.log(configuration.getSequentialPrefetchMaxSize() / (2.0 * ONE_MB))
                    / Math.log(configuration.getSequentialPrefetchBase())
                    / configuration.getSequentialPrefetchSpeed())
        + 1;
  }
}

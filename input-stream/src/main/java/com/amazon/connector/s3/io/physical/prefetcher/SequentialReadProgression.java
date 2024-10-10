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
package com.amazon.connector.s3.io.physical.prefetcher;

import static com.amazon.connector.s3.util.Constants.ONE_MB;

import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import lombok.AllArgsConstructor;

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

    // 2, 8, 32, 64
    return 2
        * ONE_MB
        * (long)
            Math.pow(
                configuration.getSequentialPrefetchBase(),
                Math.floor(configuration.getSequentialPrefetchSpeed() * generation));
  }
}

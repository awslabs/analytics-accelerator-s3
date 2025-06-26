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
package software.amazon.s3.analyticsaccelerator.request;

import lombok.AllArgsConstructor;

/**
 * Enum to help with the annotation of reads. We mark reads SYNC when they were triggered by a
 * synchronous read or ASYNC when they were to do logical or physical prefetching.
 */
@AllArgsConstructor
public enum ReadMode {
  SYNC(true),
  ASYNC(true),
  SMALL_OBJECT_PREFETCH(true),
  SEQUENTIAL_FILE_PREFETCH(true),
  DICTIONARY_PREFETCH(false),
  COLUMN_PREFETCH(false),
  REMAINING_COLUMN_PREFETCH(false),
  PREFETCH_TAIL(false),
  READ_VECTORED(false);

  private final boolean allowRequestExtension;

  /**
   * Should requests be extended for this read mode?
   *
   * <p>When the read is from the parquet prefetcher or readVectored(), we know the exact ranges we
   * want to read, so in this case don't extend the ranges.
   *
   * @return true if requests should be extended
   */
  public boolean allowRequestExtension() {
    return allowRequestExtension;
  }
}

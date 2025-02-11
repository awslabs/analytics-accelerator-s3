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

/** BlobStore memory manager */
public class MemoryManager {

  private final BlobStore blobStore;

  /**
   * Constructs an instance of memory manager
   *
   * @param blobStore blobstore
   */
  public MemoryManager(final BlobStore blobStore) {
    this.blobStore = blobStore;
  }

  /**
   * Updates memory usage of BlobStore.
   *
   * @param bytes bytes to be added to the blobstore memory usage
   */
  public void incrementBlobStoreMemoryUsage(long bytes) {
    blobStore.updateMemoryUsage(bytes);
  }
}

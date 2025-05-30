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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Closeable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.Cache;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;

/** A BlobStore is a container for Blobs and functions as a data cache. */
@SuppressFBWarnings(
    value = "SIC_INNER_SHOULD_BE_STATIC_ANON",
    justification =
        "Inner class is created very infrequently, and fluency justifies the extra pointer")
public class BlobStore implements Closeable {
  private final Map<ObjectKey, Blob> blobMap;
  private final ObjectClient objectClient;
  private final Telemetry telemetry;
  private final PhysicalIOConfiguration configuration;
  private final Cache cache;
  private final ExecutorService executorService;

  /**
   * Construct an instance of BlobStore.
   *
   * @param objectClient object client capable of interacting with the underlying object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param configuration the PhysicalIO configuration
   */
  public BlobStore(
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      @NonNull PhysicalIOConfiguration configuration) {
    this(objectClient, telemetry, configuration, null, null);
  }

  /**
   * Construct an instance of BlobStore with caching enabled.
   *
   * @param objectClient object client capable of interacting with the underlying object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param configuration the PhysicalIO configuration
   * @param cache an instance of {@link Cache} to use
   */
  public BlobStore(
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      @NonNull PhysicalIOConfiguration configuration,
      Cache cache,
      ExecutorService executorService) {
    this.objectClient = objectClient;
    this.telemetry = telemetry;
    this.blobMap =
        Collections.synchronizedMap(
            new LinkedHashMap<ObjectKey, Blob>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry<ObjectKey, Blob> eldest) {
                return this.size() > configuration.getBlobStoreCapacity();
              }
            });
    this.configuration = configuration;
    this.cache = cache;
    this.executorService = executorService;
  }

  /**
   * Opens a new blob if one does not exist or returns the handle to one that exists already.
   *
   * @param objectKey the etag and S3 URI of the object
   * @param metadata the metadata for the object we are computing
   * @param streamContext contains audit headers to be attached in the request header
   * @return the blob representing the object from the BlobStore
   */
  public Blob get(ObjectKey objectKey, ObjectMetadata metadata, StreamContext streamContext) {
    return blobMap.computeIfAbsent(
        objectKey,
        uri ->
            new Blob(
                uri,
                metadata,
                new BlockManager(
                    uri,
                    objectClient,
                    metadata,
                    telemetry,
                    configuration,
                    cache,
                    executorService,
                    streamContext),
                telemetry));
  }

  /**
   * Evicts the specified key from the cache
   *
   * @param objectKey the etag and S3 URI of the object
   * @return a boolean stating if the object existed or not
   */
  public boolean evictKey(ObjectKey objectKey) {
    return this.blobMap.remove(objectKey) != null;
  }

  /**
   * Returns the number of objects currently cached in the blobstore.
   *
   * @return an int containing the total amount of cached blobs
   */
  public int blobCount() {
    return this.blobMap.size();
  }

  /** Closes the {@link BlobStore} and frees up all resources it holds. */
  @Override
  public void close() {
    blobMap.forEach((k, v) -> v.close());
  }
}

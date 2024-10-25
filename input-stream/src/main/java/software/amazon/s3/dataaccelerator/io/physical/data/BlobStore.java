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
package software.amazon.s3.dataaccelerator.io.physical.data;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Closeable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.NonNull;
import org.omg.CORBA.TypeCodePackage.BadKind;
import software.amazon.s3.dataaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.dataaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.dataaccelerator.request.ObjectClient;
import software.amazon.s3.dataaccelerator.util.S3URI;

/** A BlobStore is a container for Blobs and functions as a data cache. */
@SuppressFBWarnings(
    value = "SIC_INNER_SHOULD_BE_STATIC_ANON",
    justification =
        "Inner class is created very infrequently, and fluency justifies the extra pointer")
public class BlobStore implements Closeable {
  private final Map<S3URI, Blob> blobMap;
  private final MetadataStore metadataStore;
  private final ObjectClient objectClient;
  private final Telemetry telemetry;
  private final PhysicalIOConfiguration configuration;

  /**
   * Construct an instance of BlobStore.
   *
   * @param metadataStore the MetadataStore storing object metadata information
   * @param objectClient object client capable of interacting with the underlying object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param configuration the PhysicalIO configuration
   */
  public BlobStore(
      @NonNull MetadataStore metadataStore,
      @NonNull ObjectClient objectClient,
      @NonNull Telemetry telemetry,
      @NonNull PhysicalIOConfiguration configuration) {
    this.metadataStore = metadataStore;
    this.objectClient = objectClient;
    this.telemetry = telemetry;
    this.blobMap =
        Collections.synchronizedMap(
            new LinkedHashMap<S3URI, Blob>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry<S3URI, Blob> eldest) {
                return this.size() > configuration.getBlobStoreCapacity();
              }
            });
    this.configuration = configuration;
  }

  /**
   * Opens a new blob if one does not exist or returns the handle to one that exists already.
   *
   * @param s3URI the S3 URI of the object
   * @return the blob representing the object from the BlobStore
   */
  public Blob get(S3URI s3URI) {
    return blobMap.computeIfAbsent(
        s3URI,
        uri ->
            new Blob(
                uri,
                metadataStore,
                new BlockManager(uri, objectClient, metadataStore, telemetry, configuration),
                telemetry));
  }

  public void removeFromBlob(S3URI s3URI) {
    Blob blob = blobMap.get(s3URI);

    if (blob != null) {
      blob.close();
    }

    blobMap.remove(s3URI);
  }

  /** Closes the {@link BlobStore} and frees up all resources it holds. */
  @Override
  public void close() {
    blobMap.forEach((k, v) -> v.close());
  }
}

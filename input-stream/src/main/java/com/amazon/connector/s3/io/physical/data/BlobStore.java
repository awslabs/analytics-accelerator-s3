package com.amazon.connector.s3.io.physical.data;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.util.S3URI;
import java.io.Closeable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** A BlobStore is a container for Blobs and functions as a data cache. */
public class BlobStore implements Closeable {

  private final Map<S3URI, Blob> blobMap;
  private final MetadataStore metadataStore;
  private final ObjectClient objectClient;
  private final PhysicalIOConfiguration configuration;

  /**
   * Construct an instance of BlobStore.
   *
   * @param metadataStore the MetadataStore storing object metadata information
   * @param objectClient object client capable of interacting with the underlying object store
   * @param configuration the PhysicalIO configuration
   */
  public BlobStore(
      MetadataStore metadataStore,
      ObjectClient objectClient,
      PhysicalIOConfiguration configuration) {
    Preconditions.checkNotNull(metadataStore, "`metadataStore` should not be null");
    Preconditions.checkNotNull(objectClient, "`objectClient` should not be null");
    Preconditions.checkNotNull("`configuration` should not be null");

    this.metadataStore = metadataStore;
    this.objectClient = objectClient;
    this.blobMap =
        Collections.synchronizedMap(
            new LinkedHashMap<S3URI, Blob>() {
              @Override
              protected boolean removeEldestEntry(final Map.Entry eldest) {
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
                new BlockManager(uri, objectClient, metadataStore, configuration)));
  }

  @Override
  public void close() {
    blobMap.forEach((k, v) -> v.close());
  }
}

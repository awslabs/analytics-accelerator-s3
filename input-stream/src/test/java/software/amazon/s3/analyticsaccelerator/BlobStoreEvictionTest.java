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
package software.amazon.s3.analyticsaccelerator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_MB;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.data.Blob;
import software.amazon.s3.analyticsaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.analyticsaccelerator.io.physical.data.MemoryManager;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class BlobStoreEvictionTest {

  @Mock private ObjectClient mockObjectClient;
  @Mock private Telemetry mockTelemetry;
  @Mock private PhysicalIOConfiguration mockConfiguration;
  @Mock private MemoryManager mockMemoryManager;

  private BlobStore blobStore;

  /** Sets up prerequisites to run the tests */
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(mockConfiguration.getMaxMemoryLimitAAL()).thenReturn((long) 9 * ONE_MB);
    BlobStore realBlobStore = new BlobStore(mockObjectClient, mockTelemetry, mockConfiguration);
    blobStore = spy(realBlobStore);
    doReturn(1024 * 1024L).when(blobStore).getBlobSize(any());
  }

  @Test
  void testBlobStoreMemoryLimitMaxMemoryLimitWhenConfigured() {
    // Given
    long configuredMaxMemory = 1000000L; // 1 MB
    PhysicalIOConfiguration config = mock(PhysicalIOConfiguration.class);
    when(config.getMaxMemoryLimitAAL()).thenReturn(configuredMaxMemory);

    // When
    BlobStore blobStore = new BlobStore(mock(ObjectClient.class), TestTelemetry.DEFAULT, config);

    // Then
    long expectedReserveMemory =
        Math.min((long) Math.ceil(0.10 * configuredMaxMemory), BlobStore.RESERVE_MEMORY);
    long expectedMaxMemoryLimit = configuredMaxMemory - expectedReserveMemory;
    assertEquals(expectedMaxMemoryLimit, blobStore.getBlobStoreMaxMemoryLimit());
  }

  @Test
  void testBlobStoreMemoryLimitMaxMemoryLimitWhenNotConfigured() {
    // Given
    PhysicalIOConfiguration config = mock(PhysicalIOConfiguration.class);
    when(config.getMaxMemoryLimitAAL()).thenReturn(Long.MAX_VALUE);

    // When
    BlobStore blobStore = new BlobStore(mock(ObjectClient.class), TestTelemetry.DEFAULT, config);

    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    long maxHeapMemory = memoryBean.getHeapMemoryUsage().getMax();
    long expectedLimit = (long) Math.ceil(0.20 * maxHeapMemory);

    // Then
    assertEquals(expectedLimit, blobStore.getBlobStoreMaxMemoryLimit());
  }

  @Test
  public void testEvictionTriggered() {
    setUp();

    int blobCount = 10;
    for (int i = 0; i < blobCount; i++) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);
      Blob blob = blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
      blob.updateActiveReaders(-1);
      blobStore.updateMemoryUsage(1024 * 1024L);
    }

    // Verify that eviction was triggered
    assertTrue(blobStore.blobCount() < blobCount);
    assertTrue(blobStore.getMemoryUsage() < blobStore.getBlobStoreMaxMemoryLimit());
  }

  @Test
  public void testEvictionPreservesActiveReaders() {
    setUp();

    for (int i = 0; i < 5; i++) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);

      blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
      blobStore.updateMemoryUsage(1024 * 1024L);
    }

    // Add more blobs to trigger eviction
    for (int i = 5; i < 10; i++) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);

      Blob blob = blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
      blob.updateActiveReaders(-1);
      blobStore.updateMemoryUsage(1024 * 1024L);
    }

    // Verify that blobs with active readers are not evicted
    assertEquals(7, blobStore.blobCount());
    for (int i = 0; i < 5; i++) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      assertNotNull(blobStore.getBlobMap().get(objectKey));
    }
  }

  @Test
  public void testEvictionOrder() {
    setUp();
    // Add blobs in a specific order
    for (int i = 0; i < 5; i++) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);

      Blob blob = blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
      blob.updateActiveReaders(-1);
      blobStore.updateMemoryUsage(1024 * 1024L);
    }

    // Access blobs in reverse order
    for (int i = 4; i >= 0; i--) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);

      Blob blob = blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
      blob.updateActiveReaders(-1);
    }

    // Add more blobs to trigger eviction
    for (int i = 5; i < 8; i++) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);

      blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
      blobStore.updateMemoryUsage(1024 * 1024L);
    }

    // Verify that the least recently used blobs were evicted
    assertNotNull(blobStore.getBlobMap().get(new ObjectKey(S3URI.of("bucket", "key0"), "etag0")));
    assertNotNull(blobStore.getBlobMap().get(new ObjectKey(S3URI.of("bucket", "key1"), "etag1")));
    assertNull(blobStore.getBlobMap().get(new ObjectKey(S3URI.of("bucket", "key2"), "etag2")));
    assertNull(blobStore.getBlobMap().get(new ObjectKey(S3URI.of("bucket", "key3"), "etag3")));
    assertNull(blobStore.getBlobMap().get(new ObjectKey(S3URI.of("bucket", "key4"), "etag4")));
  }

  @Test
  public void testAccessOrderingWithoutEviction() {
    setUp();
    // Set a larger memory limit to avoid eviction
    when(mockConfiguration.getMaxMemoryLimitAAL()).thenReturn((long) 20 * ONE_MB);

    // Add blobs in a specific order
    for (int i = 0; i < 5; i++) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);

      blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
      blobStore.updateMemoryUsage(1024 * 1024L);
    }

    // Access blobs in reverse order
    for (int i = 4; i >= 0; i--) {
      ObjectKey objectKey = new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i);
      ObjectMetadata metadata = mock(ObjectMetadata.class);
      when(metadata.getContentLength()).thenReturn(1024 * 1024L); // 1MB
      StreamContext streamContext = mock(StreamContext.class);

      blobStore.get(objectKey, metadata, streamContext, mockMemoryManager);
    }

    // Verify the order by checking the internal order of the map
    List<ObjectKey> expectedOrder = new ArrayList<>();
    for (int i = 4; i >= 0; i--) {
      expectedOrder.add(new ObjectKey(S3URI.of("bucket", "key" + i), "etag" + i));
    }

    List<ObjectKey> actualOrder = blobStore.getKeyOrder();

    assertEquals(
        expectedOrder,
        actualOrder,
        "The access order of the blobs should match the expected order");
  }
}

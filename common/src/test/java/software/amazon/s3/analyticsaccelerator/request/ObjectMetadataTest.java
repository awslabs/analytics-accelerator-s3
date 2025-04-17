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

import static org.junit.jupiter.api.Assertions.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "Null values are intentionally tested")
class ObjectMetadataTest {

  @Test
  void testValidConstruction() {
    ObjectMetadata metadata = ObjectMetadata.builder().contentLength(100).etag("test-etag").build();

    assertEquals(100, metadata.getContentLength());
    assertEquals("test-etag", metadata.getEtag());
  }

  @Test
  void testZeroContentLength() {
    ObjectMetadata metadata = ObjectMetadata.builder().contentLength(0).etag("test-etag").build();

    assertEquals(0, metadata.getContentLength());
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  void testNullEtagThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> ObjectMetadata.builder().contentLength(100).etag(null).build());
  }

  @ParameterizedTest
  @ValueSource(longs = {-1L, -100L, Long.MIN_VALUE})
  void testNegativeContentLengthThrowsException(long negativeLength) {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> ObjectMetadata.builder().contentLength(negativeLength).etag("test-etag").build());

    assertEquals("content length must be non-negative", exception.getMessage());
  }

  @Test
  void testMaxContentLength() {
    ObjectMetadata metadata =
        ObjectMetadata.builder().contentLength(Long.MAX_VALUE).etag("test-etag").build();

    assertEquals(Long.MAX_VALUE, metadata.getContentLength());
  }

  @Test
  void testEquality() {
    ObjectMetadata metadata1 =
        ObjectMetadata.builder().contentLength(100).etag("test-etag").build();

    ObjectMetadata metadata2 =
        ObjectMetadata.builder().contentLength(100).etag("test-etag").build();

    ObjectMetadata differentLength =
        ObjectMetadata.builder().contentLength(200).etag("test-etag").build();

    ObjectMetadata differentEtag =
        ObjectMetadata.builder().contentLength(100).etag("different-etag").build();

    // Test equals
    assertEquals(metadata1, metadata2);
    assertNotEquals(metadata1, differentLength);
    assertNotEquals(metadata1, differentEtag);
    assertNotEquals(metadata1, null);
    assertNotEquals(metadata1, new Object());

    // Test hashCode
    assertEquals(metadata1.hashCode(), metadata2.hashCode());
  }

  @Test
  void testToString() {
    ObjectMetadata metadata = ObjectMetadata.builder().contentLength(100).etag("test-etag").build();

    String toString = metadata.toString();
    assertTrue(toString.contains("contentLength=100"));
    assertTrue(toString.contains("etag=test-etag"));
  }

  @Test
  void testBuilder() {
    ObjectMetadata.ObjectMetadataBuilder builder = ObjectMetadata.builder();
    assertNotNull(builder);

    ObjectMetadata metadata = builder.contentLength(100).etag("test-etag").build();

    assertNotNull(metadata);
  }

  @Test
  void testSetters() {
    ObjectMetadata metadata = ObjectMetadata.builder().contentLength(100).etag("test-etag").build();

    metadata.setContentLength(200);
    assertEquals(200, metadata.getContentLength());

    metadata.setEtag("new-etag");
    assertEquals("new-etag", metadata.getEtag());
  }

  @Test
  void testCopyConstructor() {
    ObjectMetadata original = ObjectMetadata.builder().contentLength(100).etag("test-etag").build();

    ObjectMetadata copy =
        ObjectMetadata.builder()
            .contentLength(original.getContentLength())
            .etag(original.getEtag())
            .build();

    assertEquals(original, copy);
    assertNotSame(original, copy);
  }
}

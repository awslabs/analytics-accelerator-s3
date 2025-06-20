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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.util.BlockKey;
import software.amazon.s3.analyticsaccelerator.util.ObjectKey;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class DataBlockTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");
  private static final String ETAG = "RandomString";
  private static final ObjectKey TEST_OBJECT_KEY =
      ObjectKey.builder().s3URI(TEST_URI).etag(ETAG).build();
  private static final byte[] TEST_DATA_BYTES = "test-data".getBytes(StandardCharsets.UTF_8);
  private static final long READ_TIMEOUT = 5_000;

  @Test
  public void testValidConstructor() {
    Range range = new Range(0, 10);
    BlockKey blockKey = new BlockKey(TEST_OBJECT_KEY, range);

    DataBlock block =
        new DataBlock(
            blockKey, 2, mock(BlobStoreIndexCache.class), mock(Metrics.class), READ_TIMEOUT);

    assertEquals(block.getBlockKey(), blockKey);
    assertEquals(block.getGeneration(), 2);
  }

  @Test
  void testNegativeGenerationThrows() {
    Range range = new Range(0, 10);
    BlockKey blockKey = new BlockKey(TEST_OBJECT_KEY, range);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            new DataBlock(
                blockKey, -1, mock(BlobStoreIndexCache.class), mock(Metrics.class), READ_TIMEOUT));
  }
}

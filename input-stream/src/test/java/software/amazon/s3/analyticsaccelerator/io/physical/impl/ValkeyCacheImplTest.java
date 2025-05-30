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
package software.amazon.s3.analyticsaccelerator.io.physical.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.valkey.JedisCluster;
import io.valkey.exceptions.JedisException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class ValkeyCacheImplTest {

  @Test
  void testConstructorThrowsOnNullArgument() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new ValkeyCacheImpl((String) null);
        });
  }

  @Test
  void testConstructorHandlesInvalidEndpoint() {
    final String TEST_ENDPOINT = "test-endpoint";

    assertThrows(JedisException.class, () -> new ValkeyCacheImpl(TEST_ENDPOINT));
  }

  @Test
  void testGetSuccessful() {
    JedisCluster mockCluster = mock(JedisCluster.class);

    final String TEST_KEY = "test-key";
    final byte[] TEST_KEY_BYTES = TEST_KEY.getBytes(StandardCharsets.UTF_8);

    final String TEST_VALUE = "test-value";
    final byte[] TEST_VALUE_BYTES = TEST_VALUE.getBytes(StandardCharsets.UTF_8);

    when(mockCluster.get(TEST_KEY_BYTES)).thenReturn(TEST_VALUE_BYTES);

    ValkeyCacheImpl cache = new ValkeyCacheImpl(mockCluster);
    byte[] result = cache.get(TEST_KEY);

    assertArrayEquals(TEST_VALUE_BYTES, result);
    verify(mockCluster).get(TEST_KEY_BYTES);
  }

  @Test
  void testSetSuccessful() {
    JedisCluster mockCluster = mock(JedisCluster.class);

    final String TEST_KEY = "test-key";
    final byte[] TEST_KEY_BYTES = TEST_KEY.getBytes(StandardCharsets.UTF_8);

    final String TEST_VALUE = "test-value";
    final byte[] TEST_VALUE_BYTES = TEST_VALUE.getBytes(StandardCharsets.UTF_8);

    ValkeyCacheImpl cache = new ValkeyCacheImpl(mockCluster);
    cache.set(TEST_KEY, TEST_VALUE_BYTES);

    verify(mockCluster).set(TEST_KEY_BYTES, TEST_VALUE_BYTES);
  }

  @Test
  void testCloseSuccessful() {
    JedisCluster mockCluster = mock(JedisCluster.class);

    ValkeyCacheImpl cache = new ValkeyCacheImpl(mockCluster);

    cache.close();

    verify(mockCluster, times(1)).close();
  }
}

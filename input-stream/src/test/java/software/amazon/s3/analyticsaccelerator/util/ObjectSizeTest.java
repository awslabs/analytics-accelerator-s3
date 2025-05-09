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
package software.amazon.s3.analyticsaccelerator.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;

@ExtendWith(MockitoExtension.class)
class ObjectSizeTest {

  @Mock private PhysicalIOConfiguration configuration;

  @Test
  void testUtilityClass() throws Exception {
    Constructor<AnalayticsAcceleratorUtils> constructor =
        AnalayticsAcceleratorUtils.class.getDeclaredConstructor();

    constructor.newInstance();

    // Verify all methods are static
    assertTrue(
        Arrays.stream(AnalayticsAcceleratorUtils.class.getDeclaredMethods())
            .allMatch(method -> Modifier.isStatic(method.getModifiers())),
        "All methods in utility class should be static");
  }

  @Test
  void testIsSmallObject_WhenObjectSizeIsLessThanThreshold() {
    // Given
    when(configuration.isSmallObjectsPrefetchingEnabled()).thenReturn(true);
    when(configuration.getSmallObjectSizeThreshold()).thenReturn(8 * 1024 * 1024L); // 8MB
    long contentLength = 5 * 1024 * 1024L; // 5MB

    // When/Then
    assertTrue(
        AnalayticsAcceleratorUtils.isSmallObject(configuration, contentLength),
        "5MB object should be considered small when threshold is 8MB");
  }

  @Test
  void testIsSmallObject_WhenObjectSizeIsGreaterThanThreshold() {
    // Given
    when(configuration.isSmallObjectsPrefetchingEnabled()).thenReturn(true);
    when(configuration.getSmallObjectSizeThreshold()).thenReturn(8 * 1024 * 1024L); // 8MB
    long contentLength = 10 * 1024 * 1024L; // 10MB

    // When/Then
    assertFalse(
        AnalayticsAcceleratorUtils.isSmallObject(configuration, contentLength),
        "10MB object should not be considered small when threshold is 8MB");
  }
}

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
package software.amazon.s3.analyticsaccelerator.common;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class ObjectRangeTest {

  @Test
  void testDefaultConstructor() {
    ObjectRange objectRange = new ObjectRange(new CompletableFuture<>(), 0, 5);
    assertNotNull(objectRange);
  }

  @Test
  void testPreconditions() {
    assertThrows(NullPointerException.class, () -> new ObjectRange(null, 0, 5));
    assertThrows(
        IllegalArgumentException.class, () -> new ObjectRange(new CompletableFuture<>(), -1, 5));
    assertThrows(
        IllegalArgumentException.class, () -> new ObjectRange(new CompletableFuture<>(), 0, -10));
  }
}

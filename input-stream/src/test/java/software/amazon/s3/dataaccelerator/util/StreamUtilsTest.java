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
package software.amazon.s3.dataaccelerator.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import software.amazon.s3.dataaccelerator.request.ObjectContent;

public class StreamUtilsTest {

  @Test
  public void testToByteArrayWorksWithEmptyStream() {
    // Given: objectContent with an empty stream
    ObjectContent objectContent =
        ObjectContent.builder().stream(new ByteArrayInputStream(new byte[0])).build();

    // When: toByteArray is called
    byte[] buf = StreamUtils.toByteArray(objectContent);

    // Then: returned byte array is empty
    String content = new String(buf, StandardCharsets.UTF_8);
    assertEquals(0, buf.length);
    assertEquals("", content);
  }

  @Test
  public void testToByteArrayConvertsCorrectly() {
    // Given: objectContent with "Hello World" in it
    InputStream inputStream =
        new ByteArrayInputStream("Hello World".getBytes(StandardCharsets.UTF_8));
    ObjectContent objectContent = ObjectContent.builder().stream(inputStream).build();

    // When: toByteArray is called
    byte[] buf = StreamUtils.toByteArray(objectContent);

    // Then: 'Hello World' is returned
    assertEquals("Hello World", new String(buf, StandardCharsets.UTF_8));
  }
}

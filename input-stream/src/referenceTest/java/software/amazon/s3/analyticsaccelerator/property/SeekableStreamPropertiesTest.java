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
package software.amazon.s3.analyticsaccelerator.property;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import software.amazon.s3.analyticsaccelerator.arbitraries.StreamArbitraries;

/**
 * A set of high level properties that need to pass for ANY correct seekable stream implementation
 */
public class SeekableStreamPropertiesTest extends StreamArbitraries {

  @Property
  boolean positionIsInitiallyZero(@ForAll("streamSizes") int size) throws IOException {
    try (InMemoryS3SeekableInputStream s =
        new InMemoryS3SeekableInputStream("test-bucket", "test-key", size)) {

      return s.getPos() == 0;
    }
  }

  @Property
  boolean seekChangesPosition(
      @ForAll("positiveStreamSizes") int size, @ForAll("validPositions") int pos)
      throws IOException {
    try (InMemoryS3SeekableInputStream s =
        new InMemoryS3SeekableInputStream("test-bucket", "test-key", size)) {
      s.seek(pos);
      return s.getPos() == pos;
    }
  }

  @Property
  void readIncreasesPosition(
      @ForAll("sizeBiggerThanOne") int size, @ForAll("validPositions") int pos) throws IOException {
    try (InMemoryS3SeekableInputStream s =
        new InMemoryS3SeekableInputStream("test-bucket", "test-key", size)) {

      int newPos = Math.max(0, pos % size - 1);

      // Seek is correct
      s.seek(newPos);
      assertEquals(newPos, s.getPos());

      // Read increases position by 1
      s.read();
      assertEquals(newPos + 1, s.getPos());
    }
  }

  @Property
  void seekToInvalidPositionThrows(@ForAll("invalidPositions") int invalidPos) throws IOException {
    try (InMemoryS3SeekableInputStream s =
        new InMemoryS3SeekableInputStream("test-bucket", "test-key", 42)) {

      assertThrows(IllegalArgumentException.class, () -> s.seek(invalidPos));
    }
  }

  @Property
  void canCloseStreamMultipleTimes(@ForAll("streamSizes") int size) throws IOException {
    InMemoryS3SeekableInputStream s =
        new InMemoryS3SeekableInputStream("test-bucket", "test-key", size);
    s.close();
    s.close();
  }

  @Property
  void accessToClosedStreamThrows(@ForAll("streamSizes") int size) throws IOException {
    InMemoryS3SeekableInputStream s =
        new InMemoryS3SeekableInputStream("test-bucket", "test-key", size);
    s.close();
    assertThrows(IOException.class, () -> s.seek(123));
    assertThrows(IOException.class, () -> s.read());
  }
}

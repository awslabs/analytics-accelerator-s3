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
package software.amazon.s3.analyticsaccelerator.access;

import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class ExceptionCorrectnessTest extends IntegrationTestBase {

  private static final String NON_EXISTENT_OBJECT = "non-existent.bin";

  private enum ReadMethod {
    SIMPLE_READ {
      @SuppressFBWarnings(
          value = "RR_NOT_CHECKED",
          justification = "Test only cares about exception throwing, not the actual bytes read")
      void doRead(S3SeekableInputStream stream) throws IOException {
        stream.read();
      }
    },
    BUFFER_READ {
      @SuppressFBWarnings(
          value = "RR_NOT_CHECKED",
          justification = "Test only cares about exception throwing, not the actual bytes read")
      void doRead(S3SeekableInputStream stream) throws IOException {
        stream.read(new byte[10], 0, 10);
      }
    },
    TAIL_READ {
      @SuppressFBWarnings(
          value = "RR_NOT_CHECKED",
          justification = "Test only cares about exception throwing, not the actual bytes read")
      void doRead(S3SeekableInputStream stream) throws IOException {
        stream.readTail(new byte[10], 0, 10);
      }
    };

    abstract void doRead(S3SeekableInputStream stream) throws IOException;
  }

  @ParameterizedTest
  @MethodSource("provideTestParameters")
  void testNonExistentObjectThrowsRightException(S3ClientKind clientKind, ReadMethod readMethod) {
    final S3ExecutionConfiguration config = this.getS3ExecutionContext().getConfiguration();
    final S3URI nonExistentURI =
        S3URI.of(config.getBucket(), config.getPrefix() + NON_EXISTENT_OBJECT);
    S3AsyncClient s3AsyncClient = clientKind.getS3Client(getS3ExecutionContext());
    S3SeekableInputStreamFactory factory = createInputStreamFactory(s3AsyncClient);
    S3SeekableInputStream inputStream = factory.createStream(nonExistentURI);
    assertThrows(FileNotFoundException.class, () -> readMethod.doRead(inputStream));
  }

  private static Stream<Arguments> provideTestParameters() {
    return Stream.of(S3ClientKind.SDK_V2_CRT_ASYNC, S3ClientKind.SDK_V2_JAVA_ASYNC)
        .flatMap(
            clientKind ->
                Stream.of(ReadMethod.values())
                    .map(readMethod -> Arguments.of(clientKind, readMethod)));
  }

  private static S3SeekableInputStreamFactory createInputStreamFactory(
      S3AsyncClient s3AsyncClient) {
    return new S3SeekableInputStreamFactory(
        new S3SdkObjectClient(s3AsyncClient), S3SeekableInputStreamConfiguration.DEFAULT);
  }
}

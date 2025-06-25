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

import java.io.FileNotFoundException;
import java.io.IOException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class ExceptionCorrectnessTest extends IntegrationTestBase {

  private static final String NON_EXISTENT_OBJECT = "non-existent.bin";

  @ParameterizedTest
  @MethodSource("clientKinds")
  void testNonExistentObjectThrowsRightException(S3ClientKind clientKind) throws IOException {
    final S3ExecutionConfiguration config = this.getS3ExecutionContext().getConfiguration();
    final S3URI nonExistentURI =
        S3URI.of(config.getBucket(), config.getPrefix() + NON_EXISTENT_OBJECT);
    S3AsyncClient s3AsyncClient = clientKind.getS3Client(getS3ExecutionContext());
    S3SeekableInputStreamFactory factory = createInputStreamFactory(s3AsyncClient);
    assertThrows(FileNotFoundException.class, () -> factory.createStream(nonExistentURI));
  }

  private static S3SeekableInputStreamFactory createInputStreamFactory(
      S3AsyncClient s3AsyncClient) {
    return new S3SeekableInputStreamFactory(
        new S3SdkObjectClient(s3AsyncClient), S3SeekableInputStreamConfiguration.DEFAULT);
  }
}

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class SSECEncryptionTest extends IntegrationTestBase {

  @ParameterizedTest
  @MethodSource("encryptedReads")
  void testEncryptedReads(
      S3ClientKind s3ClientKind,
      S3Object s3Object,
      StreamReadPatternKind streamReadPattern,
      AALInputStreamConfigurationKind configuration)
      throws IOException {
    testReadPatternUsingSSECEncryption(s3ClientKind, s3Object, streamReadPattern, configuration);
  }

  static Stream<Arguments> encryptedReads() {
    List<S3Object> readVectoredObjects = new ArrayList<>();
    readVectoredObjects.add(S3Object.RANDOM_SSEC_ENCRYPTED_1MB);

    return argumentsFor(
        getS3ClientKinds(), readVectoredObjects, allPatterns(), readCorrectnessConfigurationKind());
  }

  private static List<AALInputStreamConfigurationKind> readCorrectnessConfigurationKind() {
    return Arrays.asList(AALInputStreamConfigurationKind.READ_CORRECTNESS);
  }
}

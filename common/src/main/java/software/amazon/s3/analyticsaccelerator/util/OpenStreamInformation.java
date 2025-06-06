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

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import software.amazon.s3.analyticsaccelerator.request.EncryptionSecrets;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.StreamAuditContext;

/**
 * Open stream information, useful for allowing the stream opening application to pass down known
 * information and callbacks when opening the stream. @Value annotation makes this class immutable
 * and automatically generates: - All-args constructor - Getters for all fields -
 * equals/hashCode/toString methods
 *
 * <p>Available getters: - getStreamContext(): Returns the stream context - getObjectMetadata():
 * Returns the object metadata - getInputPolicy(): Returns the input policy
 *
 * <p>Builder usage: OpenStreamInformation info = OpenStreamInformation.builder()
 * .streamContext(context) .objectMetadata(metadata) .inputPolicy(policy) .build();
 *
 * <p>Or use the default instance: {@code OpenStreamInformation.DEFAULT}
 */
@Builder(access = AccessLevel.PUBLIC)
@Getter
public class OpenStreamInformation {
  private final StreamAuditContext streamAuditContext;
  private final ObjectMetadata objectMetadata;
  private final InputPolicy inputPolicy;
  private final EncryptionSecrets encryptionSecrets;

  /** Default set of settings for {@link OpenStreamInformation} */
  public static final OpenStreamInformation DEFAULT = OpenStreamInformation.builder().build();
}

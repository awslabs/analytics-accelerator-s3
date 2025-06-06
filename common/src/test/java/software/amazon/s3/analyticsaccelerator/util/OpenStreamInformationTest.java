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

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.s3.analyticsaccelerator.request.EncryptionSecrets;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.StreamAuditContext;

public class OpenStreamInformationTest {

  @Test
  public void testDefaultInstance() {
    OpenStreamInformation info = OpenStreamInformation.DEFAULT;

    assertNotNull(info, "Default instance should not be null");
    assertNull(info.getStreamAuditContext(), "Default streamContext should be null");
    assertNull(info.getObjectMetadata(), "Default objectMetadata should be null");
    assertNull(info.getInputPolicy(), "Default inputPolicy should be null");
    assertNull(info.getEncryptionSecrets(), "Default encryptionSecrets should be null");
  }

  @Test
  public void testBuilderWithAllFields() {
    StreamAuditContext mockContext = Mockito.mock(StreamAuditContext.class);
    ObjectMetadata mockMetadata = Mockito.mock(ObjectMetadata.class);
    InputPolicy mockPolicy = Mockito.mock(InputPolicy.class);
    String base64Key =
        Base64.getEncoder()
            .encodeToString("32-bytes-long-key-for-testing-123".getBytes(StandardCharsets.UTF_8));
    EncryptionSecrets secrets =
        EncryptionSecrets.builder().sseCustomerKey(Optional.of(base64Key)).build();

    OpenStreamInformation info =
        OpenStreamInformation.builder()
            .streamAuditContext(mockContext)
            .objectMetadata(mockMetadata)
            .inputPolicy(mockPolicy)
            .encryptionSecrets(secrets)
            .build();

    assertSame(mockContext, info.getStreamAuditContext(), "StreamContext should match");
    assertSame(mockMetadata, info.getObjectMetadata(), "ObjectMetadata should match");
    assertSame(mockPolicy, info.getInputPolicy(), "InputPolicy should match");
    assertEquals(
        base64Key,
        info.getEncryptionSecrets().getSsecCustomerKey().get(),
        "Customer key should match");
    assertNotNull(info.getEncryptionSecrets().getSsecCustomerKeyMd5(), "MD5 should not be null");
  }

  @Test
  public void testBuilderWithPartialFields() {
    StreamAuditContext mockContext = Mockito.mock(StreamAuditContext.class);
    ObjectMetadata mockMetadata = Mockito.mock(ObjectMetadata.class);

    OpenStreamInformation info =
        OpenStreamInformation.builder()
            .streamAuditContext(mockContext)
            .objectMetadata(mockMetadata)
            .build();

    assertSame(mockContext, info.getStreamAuditContext(), "StreamContext should match");
    assertSame(mockMetadata, info.getObjectMetadata(), "ObjectMetadata should match");
    assertNull(info.getInputPolicy(), "InputPolicy should be null");
  }

  @Test
  public void testBuilderFieldRetention() {
    // Create mocks
    StreamAuditContext mockContext = Mockito.mock(StreamAuditContext.class);
    ObjectMetadata mockMetadata = Mockito.mock(ObjectMetadata.class);
    InputPolicy mockPolicy = Mockito.mock(InputPolicy.class);

    // Build object
    OpenStreamInformation info =
        OpenStreamInformation.builder()
            .streamAuditContext(mockContext)
            .objectMetadata(mockMetadata)
            .inputPolicy(mockPolicy)
            .build();

    // Verify field retention
    assertNotNull(info, "Built object should not be null");
    assertNotNull(info.getStreamAuditContext(), "StreamContext should be retained");
    assertNotNull(info.getObjectMetadata(), "ObjectMetadata should be retained");
    assertNotNull(info.getInputPolicy(), "InputPolicy should be retained");
  }

  @Test
  public void testNullFields() {
    OpenStreamInformation info =
        OpenStreamInformation.builder()
            .streamAuditContext(null)
            .objectMetadata(null)
            .inputPolicy(null)
            .build();

    assertNull(info.getStreamAuditContext(), "StreamContext should be null");
    assertNull(info.getObjectMetadata(), "ObjectMetadata should be null");
    assertNull(info.getInputPolicy(), "InputPolicy should be null");
  }

  @Test
  public void testDefaultInstanceEncryptionSecrets() {
    OpenStreamInformation info = OpenStreamInformation.DEFAULT;
    assertNull(info.getEncryptionSecrets(), "Default encryptionSecrets should be null");
  }

  @Test
  public void testBuilderWithEncryptionSecrets() {
    // Create a sample base64 encoded key
    String base64Key =
        Base64.getEncoder()
            .encodeToString("32-bytes-long-key-for-testing-123".getBytes(StandardCharsets.UTF_8));
    EncryptionSecrets secrets =
        EncryptionSecrets.builder().sseCustomerKey(Optional.of(base64Key)).build();

    OpenStreamInformation info = OpenStreamInformation.builder().encryptionSecrets(secrets).build();

    assertNotNull(info.getEncryptionSecrets(), "EncryptionSecrets should not be null");
    assertTrue(
        info.getEncryptionSecrets().getSsecCustomerKey().isPresent(),
        "Customer key should be present");
    assertEquals(
        base64Key,
        info.getEncryptionSecrets().getSsecCustomerKey().get(),
        "Customer key should match");
    assertNotNull(info.getEncryptionSecrets().getSsecCustomerKeyMd5(), "MD5 should not be null");
  }

  @Test
  public void testBuilderWithEmptyEncryptionSecrets() {
    EncryptionSecrets secrets =
        EncryptionSecrets.builder().sseCustomerKey(Optional.empty()).build();

    OpenStreamInformation info = OpenStreamInformation.builder().encryptionSecrets(secrets).build();

    assertNotNull(info.getEncryptionSecrets(), "EncryptionSecrets should not be null");
    assertFalse(
        info.getEncryptionSecrets().getSsecCustomerKey().isPresent(),
        "Customer key should be empty");
    assertNull(
        info.getEncryptionSecrets().getSsecCustomerKeyMd5(), "MD5 should be null for empty key");
  }
}

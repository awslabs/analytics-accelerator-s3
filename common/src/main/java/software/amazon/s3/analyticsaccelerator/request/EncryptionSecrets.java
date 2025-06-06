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
package software.amazon.s3.analyticsaccelerator.request;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Optional;
import lombok.Builder;
import lombok.Getter;

/**
 * Contains encryption secrets for Server-Side Encryption with Customer-Provided Keys (SSE-C). This
 * class manages the customer-provided encryption key used for SSE-C operations with Amazon S3.
 */
@Getter
public class EncryptionSecrets {

  /**
   * The customer-provided encryption key for SSE-C operations. When present, this key will be used
   * for server-side encryption. The key must be Base64 encoded and exactly 256 bits (32 bytes) when
   * decoded.
   */
  private final Optional<String> ssecCustomerKey;

  /**
   * The Base64-encoded MD5 hash of the customer key. This hash is automatically calculated from the
   * customer key and is used by Amazon S3 to verify the integrity of the encryption key during
   * transmission. Will be null if no customer key is provided.
   */
  private final String ssecCustomerKeyMd5;

  /**
   * Constructs an EncryptionSecrets instance with the specified SSE-C customer key.
   *
   * @param sseCustomerKey An Optional containing the Base64-encoded encryption key, or empty if no
   *     encryption is needed
   */
  @Builder
  public EncryptionSecrets(Optional<String> sseCustomerKey) {
    this.ssecCustomerKey = sseCustomerKey;
    this.ssecCustomerKeyMd5 =
        sseCustomerKey
            .map(
                key -> {
                  try {
                    MessageDigest md = MessageDigest.getInstance("MD5");
                    return Base64.getEncoder()
                        .encodeToString(md.digest(Base64.getDecoder().decode(key)));
                  } catch (NoSuchAlgorithmException e) {
                    throw new IllegalStateException("MD5 algorithm not available", e);
                  }
                })
            .orElse(null);
  }
}

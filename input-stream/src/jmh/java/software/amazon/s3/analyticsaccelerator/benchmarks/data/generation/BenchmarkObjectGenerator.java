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
package software.amazon.s3.analyticsaccelerator.benchmarks.data.generation;

import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;
import software.amazon.s3.analyticsaccelerator.access.S3ExecutionContext;
import software.amazon.s3.analyticsaccelerator.access.S3ObjectKind;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/** Base class for all generators */
@Getter
@RequiredArgsConstructor
public abstract class BenchmarkObjectGenerator {
  protected static final String CUSTOMER_KEY = System.getenv("CUSTOMER_KEY");
  @NonNull private final S3ExecutionContext context;
  @NonNull private final S3ObjectKind kind;

  /**
   * Generate data
   *
   * @param s3URI S3 URI to generate data into
   * @param size object size
   */
  public abstract void generate(S3URI s3URI, long size);

  /**
   * Helper method to calculate MD5 hash of customer key
   *
   * @return MD5 hash of customer key
   */
  public String calculateBase64MD5() {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      return Base64.getEncoder()
          .encodeToString(md.digest(Base64.getDecoder().decode(CUSTOMER_KEY)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("MD5 algorithm not available", e);
    }
  }

  protected String createProgressPrefix(S3URI s3URI) {
    return "[" + s3URI + "] ";
  }

  protected S3TransferManager createS3TransferManager() {
    return S3TransferManager.builder().s3Client(this.getContext().getS3CrtClient()).build();
  }

  protected void addEncryptionHeaders(
      PutObjectRequest.Builder putBuilder, S3ObjectKind encryptedKind) {
    if (getKind().equals(encryptedKind) && CUSTOMER_KEY != null) {
      putBuilder
          .sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
          .sseCustomerKey(CUSTOMER_KEY)
          .sseCustomerKeyMD5(calculateBase64MD5());
    }
  }

  protected void addEncryptionHeaders(
      HeadObjectRequest.Builder headBuilder, S3ObjectKind encryptedKind) {
    if (getKind().equals(encryptedKind) && CUSTOMER_KEY != null) {
      headBuilder
          .sseCustomerAlgorithm(ServerSideEncryption.AES256.name())
          .sseCustomerKey(CUSTOMER_KEY)
          .sseCustomerKeyMD5(calculateBase64MD5());
    }
  }

  protected void performStreamUpload(S3URI s3URI, AsyncRequestBody requestBody, long size) {
    performUpload(s3URI, null, requestBody, null, size);
  }

  protected void performFileUpload(S3URI s3URI, Path filePath, String contentType, long size) {
    performUpload(s3URI, contentType, null, filePath, size);
  }

  private void performUpload(
      S3URI s3URI, String contentType, AsyncRequestBody requestBody, Path filePath, long size) {
    String progressPrefix = createProgressPrefix(s3URI);

    try (S3TransferManager s3TransferManager = createS3TransferManager()) {
      PutObjectRequest putRequest = buildPutRequest(s3URI, contentType);
      String eTag =
          executeUpload(s3TransferManager, putRequest, requestBody, filePath, progressPrefix);
      verifyUpload(s3URI, eTag, size, progressPrefix);
    }
  }

  private PutObjectRequest buildPutRequest(S3URI s3URI, String contentType) {
    PutObjectRequest.Builder putBuilder =
        PutObjectRequest.builder().bucket(s3URI.getBucket()).key(s3URI.getKey());

    if (contentType != null) putBuilder.contentType(contentType);

    addEncryptionHeaders(putBuilder, getEncryptedKind());
    return putBuilder.build();
  }

  private String executeUpload(
      S3TransferManager s3TransferManager,
      PutObjectRequest putRequest,
      AsyncRequestBody requestBody,
      Path filePath,
      String progressPrefix) {
    System.out.println(progressPrefix + "Uploading");

    String eTag;
    if (requestBody != null) {
      UploadRequest uploadRequest =
          UploadRequest.builder().putObjectRequest(putRequest).requestBody(requestBody).build();
      eTag = s3TransferManager.upload(uploadRequest).completionFuture().join().response().eTag();
    } else {
      UploadFileRequest uploadFileRequest =
          UploadFileRequest.builder().putObjectRequest(putRequest).source(filePath).build();
      eTag =
          s3TransferManager
              .uploadFile(uploadFileRequest)
              .completionFuture()
              .join()
              .response()
              .eTag();
    }

    System.out.println(progressPrefix + "Done");
    return eTag;
  }

  private void verifyUpload(
      S3URI s3URI, String expectedETag, long expectedSize, String progressPrefix) {
    System.out.println(progressPrefix + "Verifying data...");

    HeadObjectRequest.Builder headBuilder =
        HeadObjectRequest.builder().bucket(s3URI.getBucket()).key(s3URI.getKey());

    addEncryptionHeaders(headBuilder, getEncryptedKind());

    HeadObjectResponse headResponse =
        getContext().getS3CrtClient().headObject(headBuilder.build()).join();

    if (!expectedETag.equals(headResponse.eTag())) {
      throw new IllegalStateException(
          progressPrefix
              + "Expected eTag: "
              + expectedETag
              + "; actual eTag: "
              + headResponse.eTag());
    }

    if (headResponse.contentLength() != expectedSize) {
      throw new IllegalStateException(
          progressPrefix
              + "Expected size: "
              + expectedSize
              + "; actual size: "
              + headResponse.contentLength());
    }
    System.out.println(progressPrefix + "Done");
  }

  protected abstract S3ObjectKind getEncryptedKind();
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class RequestFactoryTest {
  private static final S3URI TEST_URI = S3URI.of("test-bucket", "test-key");

  @Test
  void testConstructorWithUserAgent() {
    RequestFactory factory = new RequestFactory(new UserAgent());
    assertNotNull(factory);
  }

  @Test
  void testBuildHeadRequest() {
    HeadRequest headRequest = HeadRequest.builder().s3Uri(TEST_URI).build();

    RequestFactory factory = new RequestFactory(new UserAgent());
    HeadObjectRequest.Builder headObjectRequestBuilder =
        factory.buildHeadObjectRequest(headRequest, OpenStreamInformation.DEFAULT);

    assertNotNull(headObjectRequestBuilder);

    HeadObjectRequest headObjectRequest = headObjectRequestBuilder.build();

    assertEquals("test-key", headObjectRequest.key());
    assertEquals("test-bucket", headObjectRequest.bucket());
  }

  @Test
  void testGetRequest() {
    GetRequest getRequest =
        GetRequest.builder()
            .s3Uri(TEST_URI)
            .range(new Range(5, 10))
            .referrer(new Referrer("byte=5-10", ReadMode.READ_VECTORED))
            .etag("1234")
            .build();

    RequestFactory factory = new RequestFactory(new UserAgent());
    GetObjectRequest.Builder getObjectRequestBuilder =
        factory.getObjectRequest(getRequest, OpenStreamInformation.DEFAULT);

    assertNotNull(getObjectRequestBuilder);

    GetObjectRequest getObjectRequest = getObjectRequestBuilder.build();

    assertEquals("test-key", getObjectRequest.key());
    assertEquals("test-bucket", getObjectRequest.bucket());
    assertEquals("bytes=5-10", getObjectRequest.range());
    assertEquals("1234", getObjectRequest.ifMatch());
  }
}

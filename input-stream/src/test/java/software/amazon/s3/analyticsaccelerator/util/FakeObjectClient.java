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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.request.GetRequest;
import software.amazon.s3.analyticsaccelerator.request.HeadRequest;
import software.amazon.s3.analyticsaccelerator.request.ObjectClient;
import software.amazon.s3.analyticsaccelerator.request.ObjectContent;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.Range;

public class FakeObjectClient implements ObjectClient {

  private final String content;

  @Getter private AtomicInteger headRequestCount = new AtomicInteger();
  @Getter private AtomicInteger getRequestCount = new AtomicInteger();
  @Getter private ConcurrentLinkedDeque<Range> requestedRanges = new ConcurrentLinkedDeque<>();
  private byte[] contentBytes;
  @Getter private String etag = "RANDOM";

  /**
   * Instantiate a fake Object Client backed by some string as data.
   *
   * @param data the data making up the object
   */
  public FakeObjectClient(String data) {
    this.headRequestCount.set(0);
    this.getRequestCount.set(0);
    this.content = data;
    this.contentBytes = this.content.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public ObjectMetadata headObject(
      HeadRequest headRequest, OpenStreamInformation openStreamInformation) {
    headRequestCount.incrementAndGet();
    return ObjectMetadata.builder().contentLength(this.content.length()).etag(this.etag).build();
  }

  @Override
  public ObjectContent getObject(
      GetRequest getRequest, OpenStreamInformation openStreamInformation) {
    if (!getRequest.getEtag().equals(this.etag)) {
      throw S3Exception.builder()
          .message("At least one of the pre-conditions you specified did not hold")
          .statusCode(412)
          .build();
    }
    getRequestCount.incrementAndGet();
    requestedRanges.add(getRequest.getRange());
    return ObjectContent.builder().stream(getTestInputStream(getRequest.getRange())).build();
  }

  @Override
  public void close() {
    // noop
  }

  private InputStream getTestInputStream(Range range) {
    byte[] requestedRange =
        Arrays.copyOfRange(contentBytes, (int) range.getStart(), (int) range.getEnd() + 1);

    return new ByteArrayInputStream(requestedRange);
  }
}

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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import software.amazon.s3.analyticsaccelerator.request.GetRequest;
import software.amazon.s3.analyticsaccelerator.request.ObjectContent;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;

public class FakeStuckObjectClient extends FakeObjectClient {

  /**
   * Instantiate a fake Object Client backed by some string as data.
   *
   * @param data the data making up the object
   */
  public FakeStuckObjectClient(String data) {
    super(data);
  }

  @Override
  public CompletableFuture<ObjectContent> getObject(
      GetRequest getRequest, StreamContext streamContext) {
    CompletableFuture<ObjectContent> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new TimeoutException("Request timed out"));
    return failedFuture;
  }
}

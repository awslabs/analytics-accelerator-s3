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
/**
 * Callback interface for S3 operations in Analytics Accelerator Library. Provides methods to track
 * GET and HEAD requests.
 */
public interface RequestCallback {

  /** Called when a GET request is made. */
  void onGetRequest();

  /** Called when a HEAD request is made. */
  void onHeadRequest();

  /**
   * Called when a block prefetch is made.
   *
   * @param start start of prefetch block
   * @param end end of prefetch block
   */
  void onBlockPrefetch(long start, long end);

  /** Called when footer parsing fails. */
  void footerParsingFailed();
}

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

import lombok.AllArgsConstructor;
import lombok.Getter;

/** Kind of the stream we read from S3 */
@AllArgsConstructor
@Getter
public enum S3InputStreamKind {
  // SDK backed
  S3_SDK_GET("SDK"),
  // Ours
  S3_AAL_GET("AAL");
  private final String value;
}

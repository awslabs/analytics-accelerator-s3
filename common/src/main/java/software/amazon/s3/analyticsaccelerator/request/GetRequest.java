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

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * Object representing arguments to a GetObject call. This class helps us abstract away from S3 SDK
 * constructs.
 */
@Value
@Builder
public class GetRequest {
  @NonNull S3URI s3Uri;
  @NonNull Range range;
  @NonNull Referrer referrer;
  String etag;
}

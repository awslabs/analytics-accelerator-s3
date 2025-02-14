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

import lombok.Builder;
import lombok.Value;
import software.amazon.s3.analyticsaccelerator.request.ObjectMetadata;
import software.amazon.s3.analyticsaccelerator.request.StreamContext;

/**
 * Open file information, useful for allowing the stream opening application to pass down known
 * information and callbacks when opening the file.
 */
@Value
@Builder(toBuilder = true)
public class OpenFileInformation {
  StreamContext streamContext;
  ObjectMetadata objectMetadata;
  InputPolicy inputPolicy;

  /** Default set of settings for {@link OpenFileInformation} */
  public static final OpenFileInformation DEFAULT = OpenFileInformation.builder().build();
}

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

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/** Container used to represent an S3 object for a specific version/etag */
@Getter
@AllArgsConstructor
@Builder
public class ObjectKey {
  @NonNull S3URI s3URI;
  @NonNull String etag;

  @Override
  public final boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ObjectKey objectKey = (ObjectKey) o;
    return s3URI.equals(objectKey.s3URI) && etag.equals(objectKey.etag);
  }

  @Override
  public int hashCode() {
    return s3URI.hashCode() + etag.hashCode();
  }
}

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
package software.amazon.s3.analyticsaccelerator.io.logical.parquet;

import lombok.Data;

/** Container for storing necessary parquet column information. */
@Data
public class ColumnMetadata {
  private final int rowGroupIndex;
  private final String columnName;
  private final long dataPageOffset;
  private final long dictionaryOffset;
  private final long startPos;
  private final long compressedSize;
  private final int schemaHash;
}

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

import java.util.regex.Pattern;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;

/** A LogicalIO factory based on S3URI file extensions. */
public class ObjectFormatSelector {

  private final Pattern parquetPattern;
  private final Pattern csvPattern;
  private final Pattern jsonPattern;
  private final Pattern txtPattern;

  /**
   * Creates a new instance of {@ObjectFormatSelector}. Used to select the file format of a
   * particular object key.
   *
   * @param configuration LogicalIO configuration.
   */
  public ObjectFormatSelector(LogicalIOConfiguration configuration) {
    this.parquetPattern =
        Pattern.compile(configuration.getParquetFormatSelectorRegex(), Pattern.CASE_INSENSITIVE);
    this.csvPattern =
        Pattern.compile(configuration.getCsvFormatSelectorRegex(), Pattern.CASE_INSENSITIVE);
    this.jsonPattern =
        Pattern.compile(configuration.getJsonFormatSelectorRegex(), Pattern.CASE_INSENSITIVE);
    this.txtPattern =
        Pattern.compile(configuration.getTxtFormatSelectorRegex(), Pattern.CASE_INSENSITIVE);
  }

  /**
   * Uses a regex matcher to select the file format based on the file extension of the key.
   *
   * @param s3URI the object's S3 URI
   * @param openStreamInformation known information for this key
   * @return the file format of the object
   */
  public ObjectFormat getObjectFormat(S3URI s3URI, OpenStreamInformation openStreamInformation) {

    // If the supplied policy in open stream information is Sequential, then use the default input
    // stream, regardless of the file format (even if it's parquet!). This is important for
    // applications like DISTCP, which use a "whole_file" read policy with S3A, where they will
    // read parquet file sequentially (as they simply need to copy over the file),
    // instead of the regular parquet pattern of footer first, then specific columns etc., so our
    // parquet specific optimisations are of no use there :(

    String key = s3URI.getKey();
    if (openStreamInformation.getInputPolicy() != null
            && openStreamInformation.getInputPolicy().equals(InputPolicy.Sequential)
        || csvPattern.matcher(key).find()
        || jsonPattern.matcher(key).find()
        || txtPattern.matcher(key).find()) {
      return ObjectFormat.SEQUENTIAL;
    }

    if (parquetPattern.matcher(key).find()) {
      return ObjectFormat.PARQUET;
    }

    return ObjectFormat.DEFAULT;
  }
}

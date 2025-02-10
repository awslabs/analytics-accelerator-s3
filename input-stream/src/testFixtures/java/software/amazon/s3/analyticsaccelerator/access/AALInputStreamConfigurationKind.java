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
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;

/** Enum representing meaningful configuration samples for {@link S3ExecutionConfiguration} */
@AllArgsConstructor
@Getter
public enum AALInputStreamConfigurationKind {
  DEFAULT("DEFAULT", S3SeekableInputStreamConfiguration.DEFAULT),
  SMALL_MEMORY_LIMIT(
      "SMALL_MEMORY_LIMIT", createConfigWithMemoryLimit(100 * 1024 * 1024)); // 100 MB

  private final String name;
  private final S3SeekableInputStreamConfiguration value;

  private static S3SeekableInputStreamConfiguration createConfigWithMemoryLimit(long memoryLimit) {
    PhysicalIOConfiguration physicalIOConfig =
        PhysicalIOConfiguration.builder().maxMemoryLimitAAL(memoryLimit).build();

    return S3SeekableInputStreamConfiguration.builder()
        .physicalIOConfiguration(physicalIOConfig)
        .build();
  }
}

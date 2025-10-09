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

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;

/** Enum representing meaningful configuration samples for {@link S3ExecutionConfiguration} */
@AllArgsConstructor
@Getter
public enum AALInputStreamConfigurationKind {
  DEFAULT("DEFAULT", S3SeekableInputStreamConfiguration.DEFAULT),
  GRAY_FAILURE("GRAY_FAILURE", grayFailureConfiguration()),
  READ_CORRECTNESS("READ_CORRECTNESS", readCorrectnessConfiguration()),
  CONCURRENCY_CORRECTNESS("CONCURRENCY_CORRECTNESS", concurrencyCorrectnessConfiguration()),
  NO_RETRY("NO_RETRY", noRetryConfiguration()),
  NO_REQUEST_COALESCING("NO_COALESCING", noRequestCoalescing());

  private final String name;
  private final S3SeekableInputStreamConfiguration value;

  private static S3SeekableInputStreamConfiguration grayFailureConfiguration() {
    String configurationPrefix = "grayfailure";
    Map<String, String> customConfiguration = new HashMap<>();
    customConfiguration.put(configurationPrefix + ".physicalio.blockreadtimeout", "10000");
    customConfiguration.put(configurationPrefix + ".physicalio.blockreadretrycount", "2");
    customConfiguration.put(configurationPrefix + ".physicalio.memory.cleanup.frequency", "1");
    customConfiguration.put(
        configurationPrefix + ".physicalio.max.memory.limit", getMemoryCapacity());
    ConnectorConfiguration config =
        new ConnectorConfiguration(customConfiguration, configurationPrefix);
    return S3SeekableInputStreamConfiguration.fromConfiguration(config);
  }

  private static S3SeekableInputStreamConfiguration noRetryConfiguration() {
    String configurationPrefix = "noRetry";
    Map<String, String> customConfiguration = new HashMap<>();
    customConfiguration.put(configurationPrefix + ".physicalio.blockreadtimeout", "2000");
    customConfiguration.put(configurationPrefix + ".physicalio.blockreadretrycount", "0");
    ConnectorConfiguration config =
        new ConnectorConfiguration(customConfiguration, configurationPrefix);
    return S3SeekableInputStreamConfiguration.fromConfiguration(config);
  }

  private static S3SeekableInputStreamConfiguration readCorrectnessConfiguration() {
    String configurationPrefix = "readCorrectness";
    Map<String, String> customConfiguration = new HashMap<>();
    customConfiguration.put(
        configurationPrefix + ".physicalio.max.memory.limit", getMemoryCapacity());
    customConfiguration.put(configurationPrefix + ".physicalio.memory.cleanup.frequency", "1");
    ConnectorConfiguration config =
        new ConnectorConfiguration(customConfiguration, configurationPrefix);
    return S3SeekableInputStreamConfiguration.fromConfiguration(config);
  }

  private static S3SeekableInputStreamConfiguration concurrencyCorrectnessConfiguration() {
    String configurationPrefix = "concurrencyCorrectness";
    Map<String, String> customConfiguration = new HashMap<>();
    customConfiguration.put(
        configurationPrefix + ".physicalio.max.memory.limit", getMemoryCapacity());
    ConnectorConfiguration config =
        new ConnectorConfiguration(customConfiguration, configurationPrefix);
    return S3SeekableInputStreamConfiguration.fromConfiguration(config);
  }

  private static S3SeekableInputStreamConfiguration noRequestCoalescing() {
    String configurationPrefix = "noCoalescing";
    Map<String, String> customConfiguration = new HashMap<>();
    customConfiguration.put(configurationPrefix + ".physicalio.request.coalesce", "false");
    ConnectorConfiguration config =
        new ConnectorConfiguration(customConfiguration, configurationPrefix);
    return S3SeekableInputStreamConfiguration.fromConfiguration(config);
  }

  private static String getMemoryCapacity() {
    long maxHeapBytes = Runtime.getRuntime().maxMemory();
    double percentage = 0.0000001;
    long capacityBytes = (long) (maxHeapBytes * percentage);
    return String.valueOf(capacityBytes);
  }
}

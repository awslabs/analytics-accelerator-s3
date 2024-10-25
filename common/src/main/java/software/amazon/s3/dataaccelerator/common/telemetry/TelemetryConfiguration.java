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
package software.amazon.s3.dataaccelerator.common.telemetry;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.logging.Level;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import software.amazon.s3.dataaccelerator.common.ConnectorConfiguration;

/**
 * Configuration for {@link ConfigurableTelemetry}. The options here are on the scrappy side - not
 * everything that can be supported is supported. These can be added later if needed (e.g. {@link
 * EpochFormatter#getPattern()} and similar.
 */
@Value
@Builder
public class TelemetryConfiguration {
  // Telemetry level is standard by default
  public static final String LEVEL_KEY = "level";
  public static final String DEFAULT_LEVEL = TelemetryLevel.STANDARD.toString();

  // Console reporting is off by default
  public static final String STD_OUT_ENABLED_KEY = "std.out.enabled";
  public static final boolean DEFAULT_STD_OUT_ENABLED = false;

  // Logging reporting is on by default
  public static final String LOGGING_ENABLED_KEY = "logging.enabled";
  public static final boolean DEFAULT_LOGGING_ENABLED = false;

  // Aggregations are off by default
  public static final String AGGREGATIONS_ENABLED_KEY = "aggregations.enabled";
  public static final boolean DEFAULT_AGGREGATIONS_ENABLED = false;

  // Aggregations flush interval is not set by default
  public static final String AGGREGATIONS_FLUSH_INTERVAL_SECONDS_KEY =
      "aggregations.flush.interval.seconds";

  public static final String LOGGING_LEVEL_KEY = "logging.level";
  public static final String DEFAULT_LOGGING_LEVEL = Level.INFO.toString();

  public static final String LOGGING_NAME_KEY = "logging.name";
  public static final String DEFAULT_LOGGING_NAME = LoggingTelemetryReporter.DEFAULT_LOGGING_NAME;

  /** Telemetry level. */
  @Builder.Default String level = DEFAULT_LEVEL;
  /** Enable standard output. */
  @Builder.Default boolean stdOutEnabled = DEFAULT_STD_OUT_ENABLED;
  /** Enable logging output. */
  @Builder.Default boolean loggingEnabled = DEFAULT_LOGGING_ENABLED;
  /** Enable aggregations. */
  @Builder.Default boolean aggregationsEnabled = DEFAULT_AGGREGATIONS_ENABLED;
  /** Aggregations flush interval. */
  @Builder.Default @NonNull Optional<Duration> aggregationsFlushInterval = Optional.empty();
  /** Logging level. */
  @Builder.Default @NonNull String loggingLevel = DEFAULT_LOGGING_LEVEL;
  /** Logger name. */
  @Builder.Default @NonNull String loggingName = DEFAULT_LOGGING_NAME;

  /** Default configuration for {@link ConfigurableTelemetry}. */
  public static final TelemetryConfiguration DEFAULT = TelemetryConfiguration.builder().build();

  /**
   * Constructs {@link TelemetryConfiguration} from {@link ConnectorConfiguration} object.
   *
   * @param configuration Configuration object to generate PhysicalIOConfiguration from
   * @return LogicalIOConfiguration
   */
  public static TelemetryConfiguration fromConfiguration(
      @NonNull ConnectorConfiguration configuration) {
    // Clunky math to convert value, is present to an Optional<Duration>
    int aggregationsFlushIntervalSecondsRaw =
        configuration.getInt(AGGREGATIONS_FLUSH_INTERVAL_SECONDS_KEY, -1);
    Optional<Duration> aggregationsFlushInterval = Optional.empty();
    if (aggregationsFlushIntervalSecondsRaw > 0) {
      aggregationsFlushInterval =
          Optional.of(Duration.of(aggregationsFlushIntervalSecondsRaw, ChronoUnit.SECONDS));
    }

    return TelemetryConfiguration.builder()
        .level(configuration.getString(LEVEL_KEY, DEFAULT_LEVEL))
        .stdOutEnabled(configuration.getBoolean(STD_OUT_ENABLED_KEY, DEFAULT_STD_OUT_ENABLED))
        .loggingEnabled(configuration.getBoolean(LOGGING_ENABLED_KEY, DEFAULT_LOGGING_ENABLED))
        .aggregationsEnabled(
            configuration.getBoolean(AGGREGATIONS_ENABLED_KEY, DEFAULT_AGGREGATIONS_ENABLED))
        .aggregationsFlushInterval(aggregationsFlushInterval)
        .loggingName(configuration.getString(LOGGING_NAME_KEY, DEFAULT_LOGGING_NAME))
        .loggingLevel(configuration.getString(LOGGING_LEVEL_KEY, DEFAULT_LOGGING_LEVEL))
        .build();
  }
}

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

import java.util.HashMap;
import java.util.Map;

/**
 * A builder class for creating parameter maps used in logging operations. This class provides a
 * fluent interface for building a map of key-value pairs that can be used to structure log messages
 * with consistent parameter formatting.
 *
 * <p>Example usage:
 *
 * <pre>
 * Map<String, Object> logParams = LogParamsBuilder.create()
 *     .add("operation", "read")
 *     .add("fileName", "example.txt")
 *     .add("size", 1024)
 *     .build();
 * </pre>
 */
public class LogParamsBuilder {
  /** The internal map storing the parameters. */
  private final Map<String, Object> params = new HashMap<>();

  /**
   * Private constructor to enforce the builder pattern. Use {@link #create()} to instantiate a new
   * builder.
   */
  private LogParamsBuilder() {}

  /**
   * Creates a new instance of the LogParamsBuilder.
   *
   * @return A new LogParamsBuilder instance
   */
  public static LogParamsBuilder create() {
    return new LogParamsBuilder();
  }

  /**
   * Adds a key-value pair to the parameter map.
   *
   * @param key The parameter key (must not be null)
   * @param value The parameter value
   * @return This builder instance for method chaining
   * @throws NullPointerException if the key is null
   */
  public LogParamsBuilder add(String key, Object value) {
    params.put(key, value);
    return this;
  }

  /**
   * Builds and returns an immutable copy of the parameter map.
   *
   * @return A new HashMap containing all the added parameters
   */
  public Map<String, Object> build() {
    return new HashMap<>(params);
  }
}

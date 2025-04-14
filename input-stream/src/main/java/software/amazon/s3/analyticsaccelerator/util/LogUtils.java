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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import org.slf4j.Logger;

/**
 * Utility class for structured logging with consistent formatting across the application. Provides
 * methods for logging method entry/exit, success/failure states, and general information with
 * standardized timestamp and thread information.
 *
 * <p>The log format includes timestamp, thread information, method name, and structured parameters.
 * All logs are prefixed with context information in the format: [timestamp][ThreadName:
 * name][ThreadId: id]
 */
public class LogUtils {
  /** Formatter for timestamp in logs with millisecond precision. */
  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  /**
   * Creates a formatted string containing current timestamp and thread information.
   *
   * @return Formatted string with timestamp and thread details
   */
  public static String createThreadInfo() {
    return String.format(
        "[%s][ThreadName: %s][ThreadId: %d]",
        LocalDateTime.now().format(DATE_TIME_FORMATTER),
        Thread.currentThread().getName(),
        Thread.currentThread().getId());
  }

  /**
   * Logs an info message with method context and parameters.
   *
   * @param log The SLF4J logger instance
   * @param methodName The name of the method generating the log
   * @param params Map of parameters relevant to the log message
   * @param format Message format string
   * @param args Arguments to be formatted into the message string
   */
  public static void logInfo(
      Logger log, String methodName, Map<String, Object> params, String format, Object... args) {
    String message;

    if (args != null && args.length > 0) {
      message = String.format(format, args);
    } else {
      message = format; // Use the format string as-is if no args
    }
    log.info(
        "{} {} - Parameters: {} - Message {}", createThreadInfo(), methodName, params, message);
  }

  /**
   * Logs method entry with parameters.
   *
   * @param log The SLF4J logger instance
   * @param methodName The name of the method being entered
   * @param params Map of parameters being passed to the method
   */
  public static void logMethodEntry(Logger log, String methodName, Map<String, Object> params) {
    log.info("{} {} - Starting - Parameters: {}", createThreadInfo(), methodName, params);
  }

  /**
   * Logs successful method completion with parameters.
   *
   * @param log The SLF4J logger instance
   * @param methodName The name of the completed method
   * @param params Map of parameters relevant to the method execution
   */
  public static void logMethodSuccess(Logger log, String methodName, Map<String, Object> params) {
    log.info(
        "{} {} - Completed successfully - Parameters: {}", createThreadInfo(), methodName, params);
  }

  /**
   * Logs method failure with error details and parameters.
   *
   * @param log The SLF4J logger instance
   * @param methodName The name of the failed method
   * @param params Map of parameters relevant to the method execution
   * @param e The exception that caused the failure
   */
  public static void logMethodError(
      Logger log, String methodName, Map<String, Object> params, Exception e) {
    log.info(
        "{} {} - Failed - Parameters: {} - Error: {}",
        createThreadInfo(),
        methodName,
        params,
        e.getMessage(),
        e);
  }
}

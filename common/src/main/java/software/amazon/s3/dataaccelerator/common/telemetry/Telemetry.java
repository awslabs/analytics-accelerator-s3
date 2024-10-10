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

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import lombok.NonNull;

/** This is a set of operations that support adding telemetry for operation execution. */
public interface Telemetry extends Closeable {
  /**
   * Measures a given {@link Runnable} and record the telemetry as {@link Operation}.
   *
   * @param level telemetry level.
   * @param operationSupplier operation to record this execution as.
   * @param operationCode - code to execute.
   */
  void measure(
      @NonNull TelemetryLevel level,
      @NonNull OperationSupplier operationSupplier,
      @NonNull TelemetryAction operationCode);

  /**
   * Measures a given {@link Supplier <T>} and record the telemetry as {@link Operation}.
   *
   * @param <T> return type of the {@link Supplier<T>}.
   * @param level telemetry level.
   * @param operationSupplier operation to record this execution as.
   * @param operationCode code to execute.
   * @return the value that {@link Supplier<T>} returns.
   */
  <T> T measure(
      @NonNull TelemetryLevel level,
      @NonNull OperationSupplier operationSupplier,
      @NonNull TelemetrySupplier<T> operationCode);

  /**
   * Measures the execution of the given {@link CompletableFuture} and records the telemetry as
   * {@link Operation}. We do not currently carry the operation into the context of any
   * continuations, so any {@link Operation}s that are created in that context need to carry the
   * parenting chain.
   *
   * @param <T> - return type of the {@link CompletableFuture<T>}.
   * @param level telemetry level.
   * @param operationSupplier operation to record this execution as.
   * @param operationCode the future to measure the execution of.
   * @return an instance of {@link CompletableFuture} that returns the same result as the one passed
   *     in.
   */
  <T> CompletableFuture<T> measure(
      @NonNull TelemetryLevel level,
      @NonNull OperationSupplier operationSupplier,
      @NonNull CompletableFuture<T> operationCode);

  /**
   * This is a helper method to reduce verbosity on completed futures. Blocks on the execution on
   * {@link CompletableFuture#join()} and records the telemetry as {@link Operation}. We do not
   * currently carry the operation into the context of any continuations, so any {@link Operation}s
   * that are created in that context need to carry the parenting chain. The telemetry is only
   * recorded if the future was not completed, which is checked via {@link
   * CompletableFuture#isDone()}
   *
   * @param <T> - return type of the {@link CompletableFuture<T>}.
   * @param level telemetry level.
   * @param operationSupplier operation to record this execution as.
   * @param operationCode the future to measure the execution of.
   * @return an instance of {@link T} that returns the same result as the one passed in.
   */
  default <T> T measureJoin(
      @NonNull TelemetryLevel level,
      @NonNull OperationSupplier operationSupplier,
      @NonNull CompletableFuture<T> operationCode) {
    if (operationCode.isDone()) {
      return operationCode.join();
    } else {
      return this.measure(level, operationSupplier, operationCode::join);
    }
  }

  /**
   * Measures a given {@link Runnable} and record the telemetry as {@link Operation}. This is done
   * at {@link TelemetryLevel#CRITICAL}.
   *
   * @param operationSupplier operation to record this execution as.
   * @param operationCode - code to execute.
   */
  default void measureCritical(OperationSupplier operationSupplier, TelemetryAction operationCode) {
    measure(TelemetryLevel.CRITICAL, operationSupplier, operationCode);
  }

  /**
   * Measures a given {@link Supplier <T>} and record the telemetry as {@link Operation}. This is
   * done at {@link TelemetryLevel#CRITICAL}.
   *
   * @param <T> return type of the {@link Supplier<T>}.
   * @param operationSupplier operation to record this execution as.
   * @param operationCode code to execute.
   * @return the value that {@link Supplier<T>} returns.
   */
  default <T> T measureCritical(
      OperationSupplier operationSupplier, TelemetrySupplier<T> operationCode) {
    return measure(TelemetryLevel.CRITICAL, operationSupplier, operationCode);
  }

  /**
   * Measures the execution of the given {@link CompletableFuture} and records the telemetry as
   * {@link Operation}. We do not currently carry the operation into the context of any
   * continuations, so any {@link Operation}s that are created in that context need to carry the
   * parenting chain. This is done at {@link TelemetryLevel#CRITICAL}.
   *
   * @param <T> - return type of the {@link CompletableFuture<T>}.
   * @param operationSupplier operation to record this execution as.
   * @param operationCode the future to measure the execution of.
   * @return an instance of {@link CompletableFuture} that returns the same result as the one passed
   *     in.
   */
  default <T> CompletableFuture<T> measureCritical(
      OperationSupplier operationSupplier, CompletableFuture<T> operationCode) {
    return measure(TelemetryLevel.CRITICAL, operationSupplier, operationCode);
  }

  /**
   * This is a helper method to reduce verbosity on completed futures. Blocks on the execution on
   * {@link CompletableFuture#join()} and records the telemetry as {@link Operation}. We do not
   * currently carry the operation into the context of any continuations, so any {@link Operation}s
   * that are created in that context need to carry the parenting chain. The telemetry is only
   * recorded if the future was not completed, which is checked via {@link
   * CompletableFuture#isDone()}. This is done at {@link TelemetryLevel#CRITICAL}.
   *
   * @param <T> - return type of the {@link CompletableFuture<T>}.
   * @param operationSupplier operation to record this execution as.
   * @param operationCode the future to measure the execution of.
   * @return an instance of {@link T} that returns the same result as the one passed in.
   */
  default <T> T measureJoinCritical(
      OperationSupplier operationSupplier, CompletableFuture<T> operationCode) {
    return measureJoin(TelemetryLevel.CRITICAL, operationSupplier, operationCode);
  }

  /**
   * Measures a given {@link Runnable} and record the telemetry as {@link Operation}. This is done
   * at {@link TelemetryLevel#STANDARD}.
   *
   * @param operationSupplier operation to record this execution as.
   * @param operationCode - code to execute.
   */
  default void measureStandard(OperationSupplier operationSupplier, TelemetryAction operationCode) {
    measure(TelemetryLevel.STANDARD, operationSupplier, operationCode);
  }

  /**
   * Measures a given {@link Supplier <T>} and record the telemetry as {@link Operation}. This is
   * done at {@link TelemetryLevel#STANDARD}.
   *
   * @param <T> return type of the {@link Supplier<T>}.
   * @param operationSupplier operation to record this execution as.
   * @param operationCode code to execute.
   * @return the value that {@link Supplier<T>} returns.
   */
  default <T> T measureStandard(
      OperationSupplier operationSupplier, TelemetrySupplier<T> operationCode) {
    return measure(TelemetryLevel.STANDARD, operationSupplier, operationCode);
  }

  /**
   * Measures the execution of the given {@link CompletableFuture} and records the telemetry as
   * {@link Operation}. We do not currently carry the operation into the context of any
   * continuations, so any {@link Operation}s that are created in that context need to carry the
   * parenting chain. This is done at {@link TelemetryLevel#STANDARD}.
   *
   * @param <T> - return type of the {@link CompletableFuture<T>}.
   * @param operationSupplier operation to record this execution as.
   * @param operationCode the future to measure the execution of.
   * @return an instance of {@link CompletableFuture} that returns the same result as the one passed
   *     in.
   */
  default <T> CompletableFuture<T> measureStandard(
      OperationSupplier operationSupplier, CompletableFuture<T> operationCode) {
    return measure(TelemetryLevel.STANDARD, operationSupplier, operationCode);
  }

  /**
   * This is a helper method to reduce verbosity on completed futures. Blocks on the execution on
   * {@link CompletableFuture#join()} and records the telemetry as {@link Operation}. We do not
   * currently carry the operation into the context of any continuations, so any {@link Operation}s
   * that are created in that context need to carry the parenting chain. The telemetry is only
   * recorded if the future was not completed, which is checked via {@link
   * CompletableFuture#isDone()}. This is done at {@link TelemetryLevel#STANDARD}.
   *
   * @param <T> - return type of the {@link CompletableFuture<T>}.
   * @param operationSupplier operation to record this execution as.
   * @param operationCode the future to measure the execution of.
   * @return an instance of {@link T} that returns the same result as the one passed in.
   */
  default <T> T measureJoinStandard(
      OperationSupplier operationSupplier, CompletableFuture<T> operationCode) {
    return measureJoin(TelemetryLevel.STANDARD, operationSupplier, operationCode);
  }

  /**
   * Measures a given {@link Runnable} and record the telemetry as {@link Operation}. This is done
   * at {@link TelemetryLevel#VERBOSE}.
   *
   * @param operationSupplier operation to record this execution as.
   * @param operationCode - code to execute.
   */
  default void measureVerbose(OperationSupplier operationSupplier, TelemetryAction operationCode) {
    measure(TelemetryLevel.VERBOSE, operationSupplier, operationCode);
  }

  /**
   * Measures a given {@link Supplier <T>} and record the telemetry as {@link Operation}. This is
   * done at {@link TelemetryLevel#VERBOSE}.
   *
   * @param <T> return type of the {@link Supplier<T>}.
   * @param operationSupplier operation to record this execution as.
   * @param operationCode code to execute.
   * @return the value that {@link Supplier<T>} returns.
   */
  default <T> T measureVerbose(
      OperationSupplier operationSupplier, TelemetrySupplier<T> operationCode) {
    return measure(TelemetryLevel.VERBOSE, operationSupplier, operationCode);
  }

  /**
   * Measures the execution of the given {@link CompletableFuture} and records the telemetry as
   * {@link Operation}. We do not currently carry the operation into the context of any
   * continuations, so any {@link Operation}s that are created in that context need to carry the
   * parenting chain. This is done at {@link TelemetryLevel#VERBOSE}.
   *
   * @param <T> - return type of the {@link CompletableFuture<T>}.
   * @param operationSupplier operation to record this execution as.
   * @param operationCode the future to measure the execution of.
   * @return an instance of {@link CompletableFuture} that returns the same result as the one passed
   *     in.
   */
  default <T> CompletableFuture<T> measureVerbose(
      OperationSupplier operationSupplier, CompletableFuture<T> operationCode) {
    return measure(TelemetryLevel.VERBOSE, operationSupplier, operationCode);
  }

  /**
   * This is a helper method to reduce verbosity on completed futures. Blocks on the execution on
   * {@link CompletableFuture#join()} and records the telemetry as {@link Operation}. We do not
   * currently carry the operation into the context of any continuations, so any {@link Operation}s
   * that are created in that context need to carry the parenting chain. The telemetry is only
   * recorded if the future was not completed, which is checked via {@link
   * CompletableFuture#isDone()}. This is done at {@link TelemetryLevel#VERBOSE}.
   *
   * @param <T> - return type of the {@link CompletableFuture<T>}.
   * @param operationSupplier operation to record this execution as.
   * @param operationCode the future to measure the execution of.
   * @return an instance of {@link T} that returns the same result as the one passed in.
   */
  default <T> T measureJoinVerbose(
      OperationSupplier operationSupplier, CompletableFuture<T> operationCode) {
    return measureJoin(TelemetryLevel.VERBOSE, operationSupplier, operationCode);
  }

  /**
   * Records a measurement represented by a metric
   *
   * @param metric an instance of {@link Metric} the value applies to.
   * @param value metric value.
   */
  void measure(@NonNull Metric metric, double value);

  /** Flushes the contents of {@link Telemetry} */
  void flush();

  /** Flushes the {@link Telemetry} */
  default void close() {
    this.flush();
  }

  /**
   * Creates a new instance of {@link Telemetry} based on the configuration.
   *
   * @param configuration an instance of {@link TelemetryConfiguration}.
   * @return a new instance of {@link Telemetry}, as defined by the configuration.
   */
  static Telemetry createTelemetry(@NonNull TelemetryConfiguration configuration) {
    return new ConfigurableTelemetry(configuration);
  }

  /** An instance of {@link Telemetry} that reports nothing. */
  public static Telemetry NOOP =
      new DefaultTelemetry(
          DefaultEpochClock.DEFAULT,
          DefaultElapsedClock.DEFAULT,
          new NoOpTelemetryReporter(),
          Optional.empty(),
          TelemetryLevel.CRITICAL);
}

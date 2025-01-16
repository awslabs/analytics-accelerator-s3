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
package software.amazon.s3.analyticsaccelerator.common.telemetry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Predicate;
import java.util.function.Supplier;
import lombok.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This is a set of operations that support adding telemetry for operation execution. */
@Getter
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class DefaultTelemetry implements Telemetry {
  /** Epoch clock. Used to measure the wall time for {@link Operation} start. */
  @NonNull @Getter(AccessLevel.PACKAGE)
  private final Clock epochClock;
  /** Elapsed clock. Used to measure the duration for {@link Operation}. */
  @NonNull @Getter(AccessLevel.PACKAGE)
  private final Clock elapsedClock;
  /** Telemetry reporter */
  @NonNull @Getter(AccessLevel.PACKAGE)
  private final TelemetryReporter reporter;
  /** Telemetry aggregator */
  @NonNull @Getter(AccessLevel.PACKAGE)
  private final Optional<TelemetryDatapointAggregator> aggregator;
  /** Telemetry level */
  @NonNull @Getter private final TelemetryLevel level;

  private static final Logger LOG = LoggerFactory.getLogger(DefaultTelemetry.class);

  /** Flushes the underlying reporter */
  @Override
  public void flush() {
    this.reporter.flush();
  }

  /** Closes the underlying {@link TelemetryReporter} */
  @Override
  public void close() {
    Telemetry.super.close();
    this.aggregator.ifPresent(TelemetryDatapointAggregator::close);
    this.reporter.close();
  }

  /**
   * Measures a given {@link Runnable} and record the telemetry as {@link Operation}.
   *
   * @param level telemetry level.
   * @param operationSupplier operation to record this execution as.
   * @param operationCode - code to execute.
   */
  @SneakyThrows
  public void measure(
      @NonNull TelemetryLevel level,
      @NonNull OperationSupplier operationSupplier,
      @NonNull TelemetryAction operationCode) {
    if (produceTelemetryFor(level)) {
      measureImpl(level, operationSupplier.apply(), operationCode);
    } else {
      operationCode.apply();
    }
  }

  /**
   * Executes a given {@link Supplier<T>} and records the telemetry as {@link Operation}.
   *
   * @param <T> return type of the {@link Supplier<T>}.
   * @param level telemetry level.
   * @param operationSupplier operation to record this execution as.
   * @param operationCode code to execute.
   * @return the value that {@link Supplier<T>} returns.
   */
  @SneakyThrows
  public <T> T measure(
      @NonNull TelemetryLevel level,
      @NonNull OperationSupplier operationSupplier,
      @NonNull TelemetrySupplier<T> operationCode) {
    if (produceTelemetryFor(level)) {
      return measureImpl(level, operationSupplier.apply(), operationCode);
    } else {
      return operationCode.apply();
    }
  }

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
  @SneakyThrows
  public <T> CompletableFuture<T> measure(
      @NonNull TelemetryLevel level,
      @NonNull OperationSupplier operationSupplier,
      @NonNull CompletableFuture<T> operationCode) {
    if (produceTelemetryFor(level)) {
      return measureImpl(level, operationSupplier.apply(), operationCode);
    } else {
      return operationCode;
    }
  }

  @SneakyThrows
  public <T> T measureConditionally(
      @NonNull TelemetryLevel level,
      @NonNull OperationSupplier operationSupplier,
      @NonNull TelemetrySupplier<T> operationCode,
      @NonNull Predicate<T> shouldMeasure) {
    if (produceTelemetryFor(level)) {
      return measureConditionallyImpl(
          level, operationSupplier.apply(), operationCode, shouldMeasure);
    } else {
      return operationCode.apply();
    }
  }

  /**
   * Executes a given {@link Runnable} and record the telemetry as {@link Operation}.
   *
   * @param level level of the operation to record this execution as.
   * @param operation operation to record this execution as.
   * @param operationCode code to execute.
   */
  @SneakyThrows
  private void measureImpl(
      TelemetryLevel level, @NonNull Operation operation, TelemetryAction operationCode) {
    OperationMeasurement.OperationMeasurementBuilder builder = startMeasurement(level, operation);
    try {
      operation.getContext().pushOperation(operation);
      operationCode.apply();
      completeMeasurement(builder, Optional.empty());
    } catch (Exception error) {
      completeMeasurement(builder, Optional.of(error));
      throw error;
    } finally {
      operation.getContext().popOperation(operation);
    }
  }

  /**
   * Executes a given {@link Supplier<T>} and records the telemetry as {@link Operation}.
   *
   * @param level level of the operation to record this execution as.
   * @param operation operation to record this execution as.
   * @param operationCode code to execute.
   * @param <T> return type of the {@link Supplier<T>}.
   * @return the value that {@link Supplier<T>} returns.
   */
  @SneakyThrows
  private <T> T measureImpl(
      TelemetryLevel level, @NonNull Operation operation, TelemetrySupplier<T> operationCode) {
    OperationMeasurement.OperationMeasurementBuilder builder = startMeasurement(level, operation);
    try {
      operation.getContext().pushOperation(operation);
      T result = operationCode.apply();
      completeMeasurement(builder, Optional.empty());
      return result;
    } catch (Throwable error) {
      completeMeasurement(builder, Optional.of(error));
      throw error;
    } finally {
      operation.getContext().popOperation(operation);
    }
  }

  /**
   * Executes a given {@link Supplier<T>} and records the telemetry conditionally when the predicate
   * evaluates to true.
   *
   * @param <T> the return type of the enclosed computation
   * @param level level of the operation to record this execution as.
   * @param operation operation to record this execution as.
   * @param operationCode code to execute.
   * @param shouldMeasure predicate to test with and decide whether the operation should be
   *     recorded.
   * @return the value that {@link Supplier<T>} returns.
   */
  @SneakyThrows
  private <T> T measureConditionallyImpl(
      TelemetryLevel level,
      Operation operation,
      TelemetrySupplier<T> operationCode,
      Predicate<T> shouldMeasure) {
    OperationMeasurement.OperationMeasurementBuilder builder = startMeasurement(level, operation);
    try {
      operation.getContext().pushOperation(operation);
      T result = operationCode.apply();

      // Only complete measurement if predicate evaluates to true
      if (shouldMeasure.test(result)) {
        completeMeasurement(builder, Optional.empty());
      }

      return result;
    } catch (Throwable error) {
      completeMeasurement(builder, Optional.of(error));
      throw error;
    } finally {
      operation.getContext().popOperation(operation);
    }
  }

  /**
   * Measures the execution of the given {@link CompletableFuture} and records the telemetry as
   * {@link Operation}. We do not currently carry the operation into the context of any
   * continuations, so any {@link Operation}s that are created in that context need to carry the
   * parenting chain.
   *
   * @param level level of the operation to record this execution as.
   * @param operation operation to record this execution as.
   * @param operationCode the future to measure the execution of.
   * @param <T> - return type of the {@link CompletableFuture<T>}.
   * @return an instance of {@link CompletableFuture} that returns the same result as the one passed
   *     in.
   */
  @SneakyThrows
  private <T> CompletableFuture<T> measureImpl(
      TelemetryLevel level, Operation operation, CompletableFuture<T> operationCode) {
    OperationMeasurement.OperationMeasurementBuilder builder = startMeasurement(level, operation);
    operationCode.whenComplete(
        (result, error) -> completeMeasurement(builder, Optional.ofNullable(error)));
    return operationCode;
  }

  /**
   * Records a measurement represented by a metric
   *
   * @param metric metric to log
   * @param value metric value
   */
  @Override
  public void measure(@NonNull Metric metric, double value) {
    recordForAggregation(
        () ->
            MetricMeasurement.builder()
                .metric(metric)
                .epochTimestampNanos(getEpochClock().getCurrentTimeNanos())
                .value(value)
                .kind(MetricMeasurementKind.RAW)
                .build());
  }

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
   * @throws IOException if the underlying operation threw an IOException
   */
  @Override
  public <T> T measureJoin(
      @NonNull TelemetryLevel level,
      @NonNull OperationSupplier operationSupplier,
      @NonNull CompletableFuture<T> operationCode)
      throws IOException {
    if (operationCode.isDone()) {
      return handleCompletableFutureJoin(operationCode);
    } else {
      return this.measure(
          level, operationSupplier, () -> handleCompletableFutureJoin(operationCode));
    }
  }

  /**
   * Helper method to handle CompletableFuture join() operation and properly unwrap exceptions
   *
   * @param <T> - return type of the CompletableFuture
   * @param future the CompletableFuture to join
   * @return the result of the CompletableFuture
   * @throws IOException if the underlying future threw an IOException
   */
  private <T> T handleCompletableFutureJoin(CompletableFuture<T> future) throws IOException {
    try {
      return future.join();
    } catch (CompletionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof UncheckedIOException) {
        throw ((UncheckedIOException) cause).getCause();
      }
      throw e;
    }
  }

  /**
   * Does all the bookkeeping at the operation starts.
   *
   * @param level level of the operation being executed.
   * @param operation operation being executed.
   * @return {@link OperationMeasurement.OperationMeasurementBuilder} with all the necessary state.
   */
  private OperationMeasurement.OperationMeasurementBuilder startMeasurement(
      TelemetryLevel level, Operation operation) {
    // Create the builder
    OperationMeasurement.OperationMeasurementBuilder builder = OperationMeasurement.builder();

    // Record start times
    long epochTimestampNanos = epochClock.getCurrentTimeNanos();
    builder.operation(operation);
    builder.level(level);
    builder.epochTimestampNanos(epochTimestampNanos);
    builder.elapsedStartTimeNanos(elapsedClock.getCurrentTimeNanos());

    this.recordOperationStart(epochTimestampNanos, operation);

    return builder;
  }

  /**
   * Does all the bookkeeping at the end of the operation and returns the {@link
   * OperationMeasurement} and records the execution to the reporter.
   *
   * @param builder {@link OperationMeasurement.OperationMeasurementBuilder} representing the
   *     execution state.
   * @param error error produced during execution, if any.
   */
  private void completeMeasurement(
      OperationMeasurement.OperationMeasurementBuilder builder, Optional<Throwable> error) {
    builder.elapsedCompleteTimeNanos(elapsedClock.getCurrentTimeNanos());
    // Intentionally avoid functional style to reduce lambda invocation on the common path
    if (error.isPresent()) {
      builder.error(error.get());
    }
    OperationMeasurement operationMeasurement = builder.build();
    this.recordDatapoint(operationMeasurement);
    this.recordForAggregation(() -> operationMeasurement);
  }

  /**
   * Records the provided {@link TelemetryDatapoint}
   *
   * @param datapointMeasurement an instance of {@link TelemetryDatapointMeasurement}.
   */
  private void recordDatapoint(TelemetryDatapointMeasurement datapointMeasurement) {
    recordDatapoint(this.reporter, datapointMeasurement);
  }

  /**
   * Records the completion of {@link TelemetryDatapoint}
   *
   * @param datapointMeasurement an instance of {@link TelemetryDatapointMeasurement}.
   */
  private void recordForAggregation(Supplier<TelemetryDatapointMeasurement> datapointMeasurement) {
    aggregator.ifPresent(aggregator -> recordDatapoint(aggregator, datapointMeasurement.get()));
  }

  /**
   * Records operation completion.
   *
   * @param reporter {@link TelemetryReporter} to log the data point
   * @param datapointMeasurement an instance of {@link TelemetryDatapointMeasurement}.
   */
  private static void recordDatapoint(
      TelemetryReporter reporter, TelemetryDatapointMeasurement datapointMeasurement) {
    try {
      reporter.reportComplete(datapointMeasurement);
    } catch (Throwable error) {
      LOG.error(
          String.format(
              "Unexpected error reporting measurement for `%s`.",
              datapointMeasurement.getDatapoint()),
          error);
    }
  }

  /**
   * Records operation start.
   *
   * @param epochTimestampNanos wall clock epoch time of operation start.
   * @param operation an instance of {@link Operation}.
   */
  private void recordOperationStart(long epochTimestampNanos, Operation operation) {
    try {
      this.reporter.reportStart(epochTimestampNanos, operation);
    } catch (Throwable error) {
      LOG.error(
          String.format(
              "Unexpected error reporting operation start of `%s`.", operation.toString()),
          error);
    }
  }

  /**
   * Determines whether telemetry should be produced for this operation
   *
   * @param level {@link TelemetryLevel}
   * @return whether telemetry should be produced for this operation
   */
  private boolean produceTelemetryFor(TelemetryLevel level) {
    return level.getValue() >= this.level.getValue();
  }
}

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

import static org.junit.jupiter.api.Assertions.*;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.SpotBugsLambdaWorkaround;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class TelemetryTest {
  private static final long DEFAULT_TIMEOUT = 120_000;

  @Test
  void testCreateTelemetry() {
    try (Telemetry newTelemetry = Telemetry.createTelemetry(TelemetryConfiguration.DEFAULT)) {
      assertNotNull(newTelemetry);
      assertInstanceOf(ConfigurableTelemetry.class, newTelemetry);
    }
  }

  @Test
  void testCreateTelemetryWithNulls() {
    SpotBugsLambdaWorkaround.assertThrowsClosableResult(
        NullPointerException.class, () -> Telemetry.createTelemetry(null));
  }

  @Test
  void testNoOp() {
    try (Telemetry noopTelemetry = Telemetry.NOOP) {
      assertInstanceOf(DefaultTelemetry.class, noopTelemetry);
      DefaultTelemetry telemetry = (DefaultTelemetry) noopTelemetry;
      assertInstanceOf(NoOpTelemetryReporter.class, telemetry.getReporter());
    }
  }

  @Test
  void testMeasureJoin() throws Exception {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    try (DefaultTelemetry defaultTelemetry =
        new DefaultTelemetry(
            wallClock, elapsedClock, reporter, Optional.empty(), TelemetryLevel.CRITICAL)) {

      Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
      final CompletableFuture<Long> completableFuture = new CompletableFuture<>();
      Thread completionThread =
          new Thread(
              () -> {
                try {
                  Thread.sleep(5_000);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
                elapsedClock.tick(5);
                completableFuture.complete(42L);
              });

      elapsedClock.tick(10);
      wallClock.tick(5);

      // This will complete the future
      completionThread.start();
      defaultTelemetry.measureJoinCritical(() -> operation, completableFuture, DEFAULT_TIMEOUT);
      assertTrue(completableFuture.isDone());
      assertFalse(completableFuture.isCompletedExceptionally());
      assertEquals(42, completableFuture.get());

      assertEquals(1, reporter.getOperationCompletions().size());
      OperationMeasurement operationMeasurement =
          reporter.getOperationCompletions().stream().findFirst().get();
      assertEquals(operation, operationMeasurement.getOperation());
      assertEquals(TelemetryLevel.CRITICAL, operationMeasurement.getLevel());
      assertEquals(10, operationMeasurement.getElapsedStartTimeNanos());
      assertEquals(15, operationMeasurement.getElapsedCompleteTimeNanos());
      assertEquals(5, operationMeasurement.getElapsedTimeNanos());
      assertEquals(5, operationMeasurement.getEpochTimestampNanos());
      assertEquals(Optional.empty(), operationMeasurement.getError());

      // Try again - nothing should be recorded
      long result =
          defaultTelemetry.measureJoinStandard(() -> operation, completableFuture, DEFAULT_TIMEOUT);
      assertEquals(1, reporter.getOperationCompletions().size());
      assertEquals(42, result);
    }
  }

  @Test
  void testMeasureJoinStandard() throws Exception {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    try (DefaultTelemetry defaultTelemetry =
        new DefaultTelemetry(
            wallClock, elapsedClock, reporter, Optional.empty(), TelemetryLevel.STANDARD)) {

      Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
      final CompletableFuture<Long> completableFuture = new CompletableFuture<>();
      Thread completionThread =
          new Thread(
              () -> {
                try {
                  Thread.sleep(5_000);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
                elapsedClock.tick(5);
                completableFuture.complete(42L);
              });

      elapsedClock.tick(10);
      wallClock.tick(5);

      // This will complete the future
      completionThread.start();
      defaultTelemetry.measureJoinStandard(() -> operation, completableFuture, DEFAULT_TIMEOUT);
      assertTrue(completableFuture.isDone());
      assertFalse(completableFuture.isCompletedExceptionally());
      assertEquals(42, completableFuture.get());

      assertEquals(1, reporter.getOperationCompletions().size());
      OperationMeasurement operationMeasurement =
          reporter.getOperationCompletions().stream().findFirst().get();
      assertEquals(operation, operationMeasurement.getOperation());
      assertEquals(TelemetryLevel.STANDARD, operationMeasurement.getLevel());
      assertEquals(10, operationMeasurement.getElapsedStartTimeNanos());
      assertEquals(15, operationMeasurement.getElapsedCompleteTimeNanos());
      assertEquals(5, operationMeasurement.getElapsedTimeNanos());
      assertEquals(5, operationMeasurement.getEpochTimestampNanos());
      assertEquals(Optional.empty(), operationMeasurement.getError());

      // Try again - nothing should be recorded
      long result =
          defaultTelemetry.measureJoinStandard(() -> operation, completableFuture, DEFAULT_TIMEOUT);
      assertEquals(1, reporter.getOperationCompletions().size());
      assertEquals(42, result);
    }
  }

  @Test
  void testMeasureJoinVerbose() throws Exception {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    try (DefaultTelemetry defaultTelemetry =
        new DefaultTelemetry(
            wallClock, elapsedClock, reporter, Optional.empty(), TelemetryLevel.VERBOSE)) {

      Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();
      final CompletableFuture<Long> completableFuture = new CompletableFuture<>();
      Thread completionThread =
          new Thread(
              () -> {
                try {
                  Thread.sleep(5_000);
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
                elapsedClock.tick(5);
                completableFuture.complete(42L);
              });

      elapsedClock.tick(10);
      wallClock.tick(5);

      // This will complete the future
      completionThread.start();
      Long result =
          defaultTelemetry.measureJoinVerbose(() -> operation, completableFuture, DEFAULT_TIMEOUT);
      assertEquals(42L, result);
      assertTrue(completableFuture.isDone());
      assertFalse(completableFuture.isCompletedExceptionally());
      assertEquals(42, completableFuture.get());

      assertEquals(1, reporter.getOperationCompletions().size());
      OperationMeasurement operationMeasurement =
          reporter.getOperationCompletions().stream().findFirst().get();
      assertEquals(operation, operationMeasurement.getOperation());
      assertEquals(TelemetryLevel.VERBOSE, operationMeasurement.getLevel());
      assertEquals(10, operationMeasurement.getElapsedStartTimeNanos());
      assertEquals(15, operationMeasurement.getElapsedCompleteTimeNanos());
      assertEquals(5, operationMeasurement.getElapsedTimeNanos());
      assertEquals(5, operationMeasurement.getEpochTimestampNanos());
      assertEquals(Optional.empty(), operationMeasurement.getError());

      // Try again - nothing should be recorded
      result =
          defaultTelemetry.measureJoinStandard(() -> operation, completableFuture, DEFAULT_TIMEOUT);
      assertEquals(1, reporter.getOperationCompletions().size());
      assertEquals(42, result);
    }
  }

  @Test
  void testMeasureJoinCheckNulls() throws Exception {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    try (DefaultTelemetry defaultTelemetry =
        new DefaultTelemetry(
            wallClock, elapsedClock, reporter, Optional.empty(), TelemetryLevel.STANDARD)) {
      CompletableFuture<Long> completableFuture = new CompletableFuture<>();
      Operation operation = Operation.builder().name("name").attribute("foo", "bar").build();

      assertThrows(
          NullPointerException.class,
          () -> defaultTelemetry.measure(null, () -> operation, completableFuture));

      assertThrows(
          NullPointerException.class,
          () -> defaultTelemetry.measureJoinStandard(null, completableFuture, DEFAULT_TIMEOUT));

      assertThrows(
          NullPointerException.class,
          () ->
              defaultTelemetry.measureJoinStandard(() -> null, completableFuture, DEFAULT_TIMEOUT));
      assertThrows(
          NullPointerException.class,
          () -> defaultTelemetry.measureJoinStandard(() -> operation, null, DEFAULT_TIMEOUT));
    }
  }

  @Test
  void testAllSignatures() {
    TickingClock wallClock = new TickingClock(0L);
    TickingClock elapsedClock = new TickingClock(0L);
    CollectingTelemetryReporter reporter = new CollectingTelemetryReporter();
    try (DefaultTelemetry defaultTelemetry =
        new DefaultTelemetry(
            wallClock, elapsedClock, reporter, Optional.empty(), TelemetryLevel.VERBOSE)) {

      OperationSupplier operationSupplier =
          () -> Operation.builder().name("name").attribute("foo", "bar").build();
      TelemetrySupplier<Long> telemetrySupplier = () -> 42L;
      TelemetryAction telemetryAction = () -> {};
      CompletableFuture<Long> completableFuture = new CompletableFuture<>();
      completableFuture.complete(42L);
      // We test all fields of the OperationMeasurements elsewhere.
      // Here our main goal to make sure that operation gets logged with the right level

      // Critical
      defaultTelemetry.measureCritical(operationSupplier, telemetrySupplier);
      assertMeasuredWithLeve(TelemetryLevel.CRITICAL, reporter);
      defaultTelemetry.measureCritical(operationSupplier, telemetryAction);
      assertMeasuredWithLeve(TelemetryLevel.CRITICAL, reporter);
      defaultTelemetry.measureCritical(operationSupplier, completableFuture);
      assertMeasuredWithLeve(TelemetryLevel.CRITICAL, reporter);

      // Standard
      defaultTelemetry.measureStandard(operationSupplier, telemetrySupplier);
      assertMeasuredWithLeve(TelemetryLevel.STANDARD, reporter);
      defaultTelemetry.measureStandard(operationSupplier, telemetryAction);
      assertMeasuredWithLeve(TelemetryLevel.STANDARD, reporter);
      defaultTelemetry.measureStandard(operationSupplier, completableFuture);
      assertMeasuredWithLeve(TelemetryLevel.STANDARD, reporter);

      // Verbose
      defaultTelemetry.measureVerbose(operationSupplier, telemetrySupplier);
      assertMeasuredWithLeve(TelemetryLevel.VERBOSE, reporter);
      defaultTelemetry.measureVerbose(operationSupplier, telemetryAction);
      assertMeasuredWithLeve(TelemetryLevel.VERBOSE, reporter);
      defaultTelemetry.measureVerbose(operationSupplier, completableFuture);
      assertMeasuredWithLeve(TelemetryLevel.VERBOSE, reporter);
    }
  }

  private static void assertMeasuredWithLeve(
      TelemetryLevel level, CollectingTelemetryReporter reporter) {
    assertEquals(1, reporter.getOperationCompletions().size());
    assertEquals(level, reporter.getOperationCompletions().stream().findFirst().get().getLevel());
    reporter.clear();
  }
}

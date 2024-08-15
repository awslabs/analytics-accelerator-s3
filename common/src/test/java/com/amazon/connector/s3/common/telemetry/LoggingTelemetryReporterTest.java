package com.amazon.connector.s3.common.telemetry;

import static org.junit.jupiter.api.Assertions.*;

import java.time.ZoneId;
import java.util.Locale;
import java.util.TimeZone;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

public class LoggingTelemetryReporterTest {
  private static final long TEST_EPOCH_NANOS = 1722944779101123456L;

  @Test
  void testCreate() {
    LoggingTelemetryReporter reporter = new LoggingTelemetryReporter();
    assertEquals(LoggingTelemetryReporter.DEFAULT_LOGGING_NAME, reporter.getLoggerName());
    assertEquals(LoggingTelemetryReporter.DEFAULT_LOGGING_LEVEL, reporter.getLoggerLevel());
    assertEquals(EpochFormatter.DEFAULT, reporter.getEpochFormatter());

    Operation operation = Operation.builder().id("123").name("foo").attribute("A", 42).build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    reporter.reportStart(TEST_EPOCH_NANOS, operation);
    reporter.reportComplete(operationMeasurement);
  }

  @Test
  void testCreateWithArguments() {
    EpochFormatter epochFormatter =
        new EpochFormatter(
            "yyyy/MM/dd'T'HH;mm;ss,SSS'Z'",
            TimeZone.getTimeZone(ZoneId.of("BST", ZoneId.SHORT_IDS)),
            Locale.ENGLISH);
    LoggingTelemetryReporter reporter =
        new LoggingTelemetryReporter("foo", Level.ERROR, epochFormatter);
    assertEquals("foo", reporter.getLoggerName());
    assertEquals(Level.ERROR, reporter.getLoggerLevel());
    assertEquals(epochFormatter, reporter.getEpochFormatter());
  }

  @Test
  public void testReportComplete() {
    Operation operation = Operation.builder().name("foo").attribute("A", 42).build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    LoggingTelemetryReporter reporter = new LoggingTelemetryReporter();
    reporter.reportComplete(operationMeasurement);
  }

  @Test
  public void testReportCompleteWithException() {
    Operation operation = Operation.builder().name("foo").attribute("A", 42).build();
    OperationMeasurement operationMeasurement =
        OperationMeasurement.builder()
            .operation(operation)
            .error(new IllegalStateException("Error"))
            .epochTimestampNanos(TEST_EPOCH_NANOS)
            .elapsedStartTimeNanos(10)
            .elapsedCompleteTimeNanos(5000000)
            .build();

    LoggingTelemetryReporter reporter = new LoggingTelemetryReporter();
    reporter.reportComplete(operationMeasurement);
  }

  @Test
  void testCreateWithNulls() {
    assertThrows(
        NullPointerException.class,
        () -> {
          new LoggingTelemetryReporter(null, Level.ERROR, EpochFormatter.DEFAULT);
        });
    assertThrows(
        NullPointerException.class,
        () -> {
          new LoggingTelemetryReporter("foo", null, EpochFormatter.DEFAULT);
        });
    assertThrows(
        NullPointerException.class,
        () -> {
          new LoggingTelemetryReporter("foo", Level.ERROR, null);
        });
  }

  @Test
  public void testReportCompleteThrowsOnNull() {
    assertThrows(
        NullPointerException.class, () -> new LoggingTelemetryReporter().reportComplete(null));
  }
}

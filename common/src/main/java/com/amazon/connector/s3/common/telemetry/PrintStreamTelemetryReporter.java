package com.amazon.connector.s3.common.telemetry;

import java.io.PrintStream;
import lombok.Getter;
import lombok.NonNull;

/** Creates a new instance of {@link PrintStreamTelemetryReporter}. */
@Getter
class PrintStreamTelemetryReporter implements TelemetryReporter {
  @NonNull private final PrintStream printStream;
  @NonNull private final EpochFormatter epochFormatter;

  /**
   * Creates a new instance of {@link PrintStreamTelemetryReporter}.
   *
   * @param printStream an instance of {@link PrintStream to output to}.
   * @param epochFormatter an instance of {@link EpochFormatter to use to format epochs}.
   */
  public PrintStreamTelemetryReporter(
      @NonNull PrintStream printStream, @NonNull EpochFormatter epochFormatter) {
    this.printStream = printStream;
    this.epochFormatter = epochFormatter;
  }

  /**
   * Creates a new instance of {@link PrintStreamTelemetryReporter} with default {@link
   * EpochFormatter}.
   *
   * @param printStream the {@link PrintStream} to output telemetry to.
   */
  public PrintStreamTelemetryReporter(PrintStream printStream) {
    this(printStream, EpochFormatter.DEFAULT);
  }

  /**
   * Outputs the current contents of {@link OperationMeasurement} into a {@link PrintStream}.
   *
   * @param operationMeasurement operation execution.
   */
  @Override
  public void report(@NonNull OperationMeasurement operationMeasurement) {
    printStream.println(operationMeasurement.toString(epochFormatter));
  }
}
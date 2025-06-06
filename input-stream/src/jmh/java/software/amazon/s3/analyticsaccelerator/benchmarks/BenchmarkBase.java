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
package software.amazon.s3.analyticsaccelerator.benchmarks;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;
import org.openjdk.jmh.annotations.*;
import software.amazon.s3.analyticsaccelerator.access.*;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;

/**
 * Base class for benchmarks that iterate through the client types and stream types All derived
 * benchmarks are going to be run with Java Async and CRT clients, as well as SDK GET streams as the
 * seekable streams
 */
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@State(Scope.Benchmark)
public abstract class BenchmarkBase extends ExecutionBase {
  // NOTE: be super careful with @Params naming, JMH seems to order the tests
  // by ordering the field names alphabetically, affecting grouping that is shown by the reports
  // In this case, id we want to compare performance for each object size (which we do)
  // `object` needs to come first.

  // TODO: add more form factors
  @Param({"RANDOM_1MB", "RANDOM_4MB"})
  public S3Object object;

  @NonNull private final AtomicReference<S3ExecutionContext> s3ExecutionContext = new AtomicReference<>();
  /**
   * Sets up the benchmarking context
   *
   * @throws IOException thrown on IO error
   */
  @Setup(Level.Trial)
  public void setUp() throws IOException {
    this.s3ExecutionContext.getAndSet(
        new S3ExecutionContext(S3ExecutionConfiguration.fromEnvironment()));
  }

  /**
   * Tears down benchmarking context
   *
   * @throws IOException thrown on IO error
   */
  @TearDown(Level.Trial)
  public void tearDown() throws IOException {
    this.s3ExecutionContext.getAndSet(null).close();
  }

  /**
   * Returns benchmark context
   *
   * @return benchmark context
   */
  public S3ExecutionContext getS3ExecutionContext() {
    return this.s3ExecutionContext.get();
  }

  /**
   * Benchmarks should override this to return the object to benchmark against on each iteration
   *
   * @return object to benchmark against
   */
  protected abstract S3Object getObject();

  /**
   * Benchmarks should override this to return the read pattern kind
   *
   * @return read pattern kind
   */
  protected abstract StreamReadPatternKind getReadPatternKind();

  /**
   * Returns current client kind
   *
   * @return {@link S3ClientKind}
   */
  protected abstract S3ClientKind getClientKind();

  /**
   * Benchmarks can override this to return the {@link AALInputStreamConfigurationKind}
   *
   * @return {@link AALInputStreamConfigurationKind}
   */
  protected AALInputStreamConfigurationKind getDATInputStreamConfigurationKind() {
    return AALInputStreamConfigurationKind.DEFAULT;
  }

  @Benchmark
  public void execute() throws Exception {
    this.executeBenchmark();
  }

  /**
   * Benchmark specific execution
   *
   * @throws Exception any error thrown
   */
  protected abstract void executeBenchmark() throws Exception;

  /**
   * Executes the pattern on DAT based on the contextual parameters.
   *
   * @throws IOException if IO error is thrown
   */
  protected void executeReadPatternOnDAT() throws IOException {
    S3Object s3Object = this.getObject();
    executeReadPatternOnAAL(
        this.getClientKind(),
        s3Object,
        this.getReadPatternKind().getStreamReadPattern(s3Object),
        // Use default configuration
        this.getDATInputStreamConfigurationKind(),
        Optional.empty(),
        OpenStreamInformation.DEFAULT);
  }

  /**
   * Executes the pattern on S3 SDK based on the contextual parameters.
   *
   * @throws IOException if IO error is thrown
   */
  protected void executeReadPatternDirectly() throws IOException {
    S3Object s3Object = this.getObject();
    executeReadPatternDirectly(
        this.getClientKind(),
        s3Object,
        this.getReadPatternKind().getStreamReadPattern(s3Object),
        Optional.empty(),
        OpenStreamInformation.DEFAULT);
  }
}

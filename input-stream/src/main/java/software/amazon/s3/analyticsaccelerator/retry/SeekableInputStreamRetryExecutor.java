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
package software.amazon.s3.analyticsaccelerator.retry;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;

/**
 * Implementation of RetryExecutor that uses Failsafe library internally.
 *
 * <p>This class encapsulates all Failsafe-specific code to hide the dependency from AAL users. It
 * supports multiple retry policies that are applied in sequence, allowing for complex retry
 * strategies such as combining different policies for different types of failures.
 *
 * <p>The executor maintains a list of retry policies and creates a Failsafe executor that applies
 * these policies when executing operations that may fail.
 *
 * @param <R> the result type of operations executed by this retry executor
 */
public class SeekableInputStreamRetryExecutor<R> implements RetryExecutor<R> {
  private final List<RetryPolicy<R>> retryPolicies;
  FailsafeExecutor<R> failsafeExecutor;

  /**
   * Creates a no-op executor with no retry policies.
   *
   * <p>Operations executed with this executor will not be retried on failure. This constructor is
   * primarily used for testing or scenarios where retry behavior is not desired.
   */
  public SeekableInputStreamRetryExecutor() {
    this.retryPolicies = new ArrayList<>();
    this.failsafeExecutor = Failsafe.none();
  }

  /**
   * Creates a retry executor with a single retry policy based on the provided configuration.
   *
   * <p>The retry policy is configured using settings from the PhysicalIOConfiguration, including
   * retry count and timeout values.
   *
   * @param configuration the physical IO configuration containing retry settings
   * @throws NullPointerException if configuration is null
   */
  public SeekableInputStreamRetryExecutor(PhysicalIOConfiguration configuration) {
    this.retryPolicies = Collections.singletonList(RetryPolicy.<R>builder(configuration).build());
    this.failsafeExecutor = Failsafe.with(getDelegates());
  }

  /**
   * Creates a retry executor with multiple retry policies.
   *
   * <p>The policies are applied in the order they are provided, with the outerPolicy being applied
   * first, followed by the additional policies. This allows for layered retry strategies where
   * different policies handle different aspects of failure recovery.
   *
   * @param outerPolicy the primary retry policy to apply
   * @param policies additional retry policies to apply after the outer policy
   * @throws NullPointerException if outerPolicy is null
   */
  @SafeVarargs
  @SuppressWarnings("varargs")
  public SeekableInputStreamRetryExecutor(RetryPolicy<R> outerPolicy, RetryPolicy<R>... policies) {
    Preconditions.checkNotNull(outerPolicy);
    this.retryPolicies = new ArrayList<>();
    this.retryPolicies.add(outerPolicy);
    if (policies != null && policies.length > 0) {
      this.retryPolicies.addAll(Arrays.asList(policies));
    }
    this.failsafeExecutor = Failsafe.with(getDelegates());
  }

  /**
   * Executes a runnable with retry logic using Failsafe internally.
   *
   * @param runnable The operation to execute
   * @throws IOException if the operation fails after all retries
   */
  @Override
  public void executeWithRetry(IORunnable runnable) throws IOException {
    try {
      this.failsafeExecutor.run(runnable::run);
    } catch (Exception e) {
      throw new IOException("Failed to execute operation with retries", e);
    }
  }

  @Override
  public R getWithRetry(IOSupplier<R> supplier) throws IOException {
    try {
      return this.failsafeExecutor.get(supplier::get);
    } catch (Exception e) {
      throw new IOException("Failed to execute operation with retries", e);
    }
  }

  /**
   * Extracts the underlying Failsafe retry policies from the wrapper retry policies.
   *
   * <p>This method is used internally to convert the list of RetryPolicy wrappers into the actual
   * Failsafe RetryPolicy instances that can be used with the Failsafe executor.
   *
   * @return a list of Failsafe RetryPolicy instances
   */
  private List<dev.failsafe.RetryPolicy<R>> getDelegates() {
    return this.retryPolicies.stream().map(RetryPolicy::getDelegate).collect(Collectors.toList());
  }
}

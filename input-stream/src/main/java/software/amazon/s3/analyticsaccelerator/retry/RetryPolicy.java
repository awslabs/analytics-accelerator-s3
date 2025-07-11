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

import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;

/**
 * Interface for retry policies that delegate to Failsafe retry policies.
 *
 * <p>This interface provides a wrapper around Failsafe's retry functionality, allowing for
 * consistent retry behavior across the analytics accelerator. Retry policies can be configured with
 * custom retry counts, timeouts, and exception handling rules.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * RetryPolicy<String> policy = RetryPolicy.<String>builder()
 *     .withMaxRetries(3)
 *     .handle(IOException.class)
 *     .build();
 *
 * RetryExecutor<String> executor = new SeekableInputStreamRetryExecutor(policy);
 * executor.executeWithRetry( () -> myIOOperation()); // will be retried 3 times and throw IOException if fails
 *
 * }</pre>
 *
 * @param <R> the result type of operations executed with this retry policy
 */
public interface RetryPolicy<R> {

  /**
   * Creates a new RetryPolicyBuilder with default configuration
   * using @link{PhysicalIOConfiguration.DEFAULT}
   *
   * @param <R> the result type
   * @return a new RetryPolicyBuilder instance with default settings
   */
  static <R> RetryPolicyBuilder<R> builder() {
    return new RetryPolicyBuilder<>();
  }

  /**
   * Creates a new RetryPolicyBuilder configured based on the provided PhysicalIOConfiguration.
   *
   * @param configuration the physical IO configuration containing retry settings
   * @param <R> the result type
   * @return a new RetryPolicyBuilder instance configured from the provided settings
   * @throws NullPointerException if configuration is null
   */
  static <R> RetryPolicyBuilder<R> builder(PhysicalIOConfiguration configuration) {
    return new RetryPolicyBuilder<>(configuration);
  }

  /**
   * Creates a RetryPolicy with default configuration.
   *
   * <p>This is a convenience method equivalent to calling {@code RetryPolicy.<T>builder().build()}.
   *
   * @param <R> the result type
   * @return a RetryPolicy instance with default settings
   */
  static <R> RetryPolicy<R> ofDefaults() {
    return RetryPolicy.<R>builder().build();
  }

  /**
   * Gets the underlying Failsafe retry policy that this wrapper delegates to.
   *
   * <p>This method provides access to the actual Failsafe RetryPolicy instance for advanced use
   * cases where direct interaction with the Failsafe API is required. Most users should prefer the
   * higher-level methods provided by this interface.
   *
   * @return the underlying Failsafe RetryPolicy instance
   */
  dev.failsafe.RetryPolicy<R> getDelegate();
}

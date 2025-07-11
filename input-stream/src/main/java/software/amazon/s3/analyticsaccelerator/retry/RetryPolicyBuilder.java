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

import java.time.Duration;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;

/**
 * Builder for creating RetryPolicy instances that delegate to Failsafe retry policies.
 *
 * <p>This builder provides a fluent API for configuring retry policies with various settings such
 * as maximum retry attempts, delays between retries, timeout durations, and exception handling
 * rules. The builder uses the underlying Failsafe library.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * RetryPolicy<String> policy = new RetryPolicy<String>.builder()
 *     .withMaxRetries(5)
 *     .withDelay(Duration.ofSeconds(2))
 *     .withMaxDuration(Duration.ofMinutes(1))
 *     .handle(IOException.class, TimeoutException.class)
 *     .build();
 * }</pre>
 *
 * <p>The builder is not thread-safe and should not be shared between threads without external
 * synchronization.
 *
 * @param <R> the result type of operations that will be executed with the retry policy
 */
public class RetryPolicyBuilder<R> {

  private final dev.failsafe.RetryPolicyBuilder<R> delegateBuilder;

  protected RetryPolicyBuilder() {
    this(PhysicalIOConfiguration.DEFAULT);
  }

  protected RetryPolicyBuilder(PhysicalIOConfiguration configuration) {
    this.delegateBuilder = dev.failsafe.RetryPolicy.builder();
    delegateBuilder.withMaxDuration(Duration.ofMillis(configuration.getBlockReadTimeout()));
    delegateBuilder.withMaxRetries(configuration.getBlockReadRetryCount());
  }

  /**
   * Sets the maximum number of retry attempts.
   *
   * @param maxRetries the maximum number of retries
   * @return this builder
   */
  public RetryPolicyBuilder<R> withMaxRetries(int maxRetries) {
    delegateBuilder.withMaxRetries(maxRetries);
    return this;
  }

  /**
   * Sets the delay between retry attempts.
   *
   * @param delay the delay duration
   * @return this builder
   */
  public RetryPolicyBuilder<R> withDelay(Duration delay) {
    delegateBuilder.withDelay(delay);
    return this;
  }

  /**
   * Sets the maximum duration for all retry attempts.
   *
   * <p>This sets an overall timeout for the entire retry operation, including all retry attempts
   * and delays. If this duration is exceeded, no further retries will be attempted regardless of
   * the maximum retry count.
   *
   * @param timeout the maximum duration for all retry attempts
   * @return this builder for method chaining
   * @throws NullPointerException if timeout is null
   */
  public RetryPolicyBuilder<R> withMaxDuration(Duration timeout) {
    delegateBuilder.withMaxDuration(timeout);
    return this;
  }

  /**
   * Specifies which exceptions should trigger a retry.
   *
   * @param exception the exception class
   * @return this builder
   */
  public RetryPolicyBuilder<R> handle(Class<? extends Throwable> exception) {
    delegateBuilder.handle(exception);
    return this;
  }

  /**
   * Specifies which exceptions should trigger a retry.
   *
   * @param exceptions the exception class
   * @return this builder
   */
  @SafeVarargs
  @SuppressWarnings("varargs")
  public final RetryPolicyBuilder<R> handle(Class<? extends Throwable>... exceptions) {
    delegateBuilder.handle(exceptions);
    return this;
  }

  /**
   * Builds the RetryPolicy with the configured settings.
   *
   * @return a new RetryPolicy instance
   */
  public RetryPolicy<R> build() {
    dev.failsafe.RetryPolicy<R> delegate = delegateBuilder.build();

    return new RetryPolicy<R>() {
      @Override
      public dev.failsafe.RetryPolicy<R> getDelegate() {
        return delegate;
      }
    };
  }
}

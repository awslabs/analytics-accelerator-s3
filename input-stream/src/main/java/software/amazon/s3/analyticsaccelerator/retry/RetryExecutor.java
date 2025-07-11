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

import java.io.IOException;

/**
 * Interface for executing operations with retry logic. We do not expect consumers of AAL to
 * instantiate an implementation of this interface themselves. This is for AAL's internal use only.
 *
 * <p>This interface provides a contract for executing operations that may fail and need to be
 * retried according to a configured retry policy. Implementations should handle the retry logic
 * internally and only throw exceptions when all retry attempts have been exhausted.
 *
 * @param <R> the result type of operations executed by this retry executor
 */
public interface RetryExecutor<R> {
  /**
   * Executes a runnable with retry logic according to the configured retry policy.
   *
   * <p>The operation will be retried according to the retry policy configuration, including maximum
   * retry attempts, delays between retries, and which exceptions should trigger retries. If all
   * retry attempts are exhausted, the final exception will be wrapped in an IOException and thrown.
   *
   * @param runnable the operation to execute, which may throw IOException
   * @throws IOException if the operation fails after all retry attempts are exhausted
   * @throws NullPointerException if runnable is null
   */
  void executeWithRetry(IORunnable runnable) throws IOException;

  /**
   * Executes a supplier with retry logic according to the configured retry policy.
   *
   * <p>The operation will be retried according to the retry policy configuration, including maximum
   * retry attempts, delays between retries, and which exceptions should trigger retries. If all
   * retry attempts are exhausted, the final exception will be wrapped in an IOException and thrown.
   *
   * @param supplier the operation to execute, returns <R>, may throw IOException
   * @return R result of the supplier.
   * @throws IOException if the operation fails due to an I/O error
   */
  R getWithRetry(IOSupplier<R> supplier) throws IOException;

  /** Functional interface for operations that can throw IOException. */
  @FunctionalInterface
  interface IORunnable {
    /**
     * Runs the operation.
     *
     * <p>This method should contain the logic that needs to be executed with retry support. The
     * operation should be idempotent as it may be executed multiple times in case of failures.
     *
     * @throws IOException if the operation fails due to an I/O error
     */
    void run() throws IOException;
  }

  /** Functional interface for operations that can throw IOException. */
  @FunctionalInterface
  interface IOSupplier<R> {
    /**
     * Runs the operation.
     *
     * <p>This method should contain the logic that needs to be executed with retry support. The
     * operation should be idempotent as it may be executed multiple times in case of failures.
     *
     * @return R result of the supplier.
     * @throws IOException if the operation fails due to an I/O error
     */
    R get() throws IOException;
  }
}

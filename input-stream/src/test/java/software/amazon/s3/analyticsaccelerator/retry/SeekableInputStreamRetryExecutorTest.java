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

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;

class SeekableInputStreamRetryExecutorTest {

  @Test
  void testNoArgsConstructor() {
    SeekableInputStreamRetryExecutor<String> executor = new SeekableInputStreamRetryExecutor<>();
    assertNotNull(executor);
  }

  @Test
  void testConfigurationConstructor() {
    PhysicalIOConfiguration config = PhysicalIOConfiguration.DEFAULT;
    SeekableInputStreamRetryExecutor<String> executor =
        new SeekableInputStreamRetryExecutor<>(config);
    assertNotNull(executor);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testPolicyConstructor() {
    RetryPolicy<String> policy = RetryPolicy.ofDefaults();
    SeekableInputStreamRetryExecutor<String> executor =
        new SeekableInputStreamRetryExecutor<>(policy);
    assertNotNull(executor);
  }

  @Test
  @SuppressWarnings("unchecked")
  void testPolicyConstructorWithMultiplePolicies() {
    RetryPolicy<String> policy1 = RetryPolicy.ofDefaults();
    RetryPolicy<String> policy2 = RetryPolicy.ofDefaults();
    SeekableInputStreamRetryExecutor<String> executor =
        new SeekableInputStreamRetryExecutor<>(policy1, policy2);
    assertNotNull(executor);
  }

  @Test
  void testPolicyConstructorWithNullOuterPolicyThrowsException() {
    assertThrows(
        NullPointerException.class, () -> new SeekableInputStreamRetryExecutor<String>(null));
  }

  @Test
  void testExecuteWithRetrySuccess() throws IOException {
    SeekableInputStreamRetryExecutor<String> executor = new SeekableInputStreamRetryExecutor<>();
    AtomicInteger counter = new AtomicInteger(0);

    executor.executeWithRetry(counter::incrementAndGet);

    assertEquals(1, counter.get());
  }

  @Test
  void testExecuteWithRetryWrapsException() {
    SeekableInputStreamRetryExecutor<String> executor = new SeekableInputStreamRetryExecutor<>();

    IOException exception =
        assertThrows(
            IOException.class,
            () ->
                executor.executeWithRetry(
                    () -> {
                      throw new RuntimeException("Test exception");
                    }));

    assertEquals("Failed to execute operation with retries", exception.getMessage());
    assertNotNull(exception.getCause());
  }

  @Test
  void testExecuteWithRetryIOException() {
    SeekableInputStreamRetryExecutor<String> executor = new SeekableInputStreamRetryExecutor<>();

    IOException exception =
        assertThrows(
            IOException.class,
            () ->
                executor.executeWithRetry(
                    () -> {
                      throw new IOException("Original IO exception");
                    }));

    assertEquals("Failed to execute operation with retries", exception.getMessage());
  }

  @Test
  void testExecuteWithRetryWithConfiguration() throws IOException {
    PhysicalIOConfiguration config = PhysicalIOConfiguration.DEFAULT;
    SeekableInputStreamRetryExecutor<String> executor =
        new SeekableInputStreamRetryExecutor<>(config);
    AtomicInteger counter = new AtomicInteger(0);

    executor.executeWithRetry(counter::incrementAndGet);

    assertEquals(1, counter.get());
  }

  @Test
  void testGetWithRetrySuccess() throws IOException {
    SeekableInputStreamRetryExecutor<String> executor = new SeekableInputStreamRetryExecutor<>();
    String expected = "test result";

    String result = executor.getWithRetry(() -> expected);

    assertEquals(expected, result);
  }

  @Test
  void testGetWithRetryWrapsException() {
    SeekableInputStreamRetryExecutor<String> executor = new SeekableInputStreamRetryExecutor<>();

    IOException exception =
        assertThrows(
            IOException.class,
            () ->
                executor.getWithRetry(
                    () -> {
                      throw new RuntimeException("Test exception");
                    }));

    assertEquals("Failed to execute operation with retries", exception.getMessage());
    assertNotNull(exception.getCause());
  }

  @Test
  void testGetWithRetryIOException() {
    SeekableInputStreamRetryExecutor<String> executor = new SeekableInputStreamRetryExecutor<>();

    IOException exception =
        assertThrows(
            IOException.class,
            () ->
                executor.getWithRetry(
                    () -> {
                      throw new IOException("Original IO exception");
                    }));

    assertEquals("Failed to execute operation with retries", exception.getMessage());
  }

  @Test
  void testGetWithRetryWithConfiguration() throws IOException {
    PhysicalIOConfiguration config = PhysicalIOConfiguration.DEFAULT;
    SeekableInputStreamRetryExecutor<Integer> executor =
        new SeekableInputStreamRetryExecutor<>(config);
    Integer expected = 42;

    Integer result = executor.getWithRetry(() -> expected);

    assertEquals(expected, result);
  }

  @Test
  void testGetWithRetryNullResult() throws IOException {
    SeekableInputStreamRetryExecutor<String> executor = new SeekableInputStreamRetryExecutor<>();

    String result = executor.getWithRetry(() -> null);

    assertNull(result);
  }

  @Test
  void testGetWithRetrySucceedsOnThirdAttempt() throws IOException {
    PhysicalIOConfiguration config = PhysicalIOConfiguration.DEFAULT;
    SeekableInputStreamRetryExecutor<Integer> executor =
        new SeekableInputStreamRetryExecutor<>(config);
    AtomicInteger retryCount = new AtomicInteger(0);

    Integer result = executor.getWithRetry(() -> failTwiceThenSucceed(retryCount));

    assertEquals(1, result);
    assertEquals(3, retryCount.get());
  }

  private Integer failTwiceThenSucceed(AtomicInteger counter) throws IOException {
    int attempt = counter.incrementAndGet();
    if (attempt <= 2) {
      throw new IOException("Attempt " + attempt + " failed");
    }
    return 1;
  }
}

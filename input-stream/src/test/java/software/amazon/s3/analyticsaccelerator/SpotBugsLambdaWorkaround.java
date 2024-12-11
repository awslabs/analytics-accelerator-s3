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
package software.amazon.s3.analyticsaccelerator;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Closeable;
import lombok.SneakyThrows;
import org.junit.jupiter.api.function.ThrowingSupplier;

/**
 * This class is here to help work around certain SpotBugs quirks. Specifically, SpotBugs don't
 * allow to apply suppressions to Lambdas
 */
public final class SpotBugsLambdaWorkaround {
  /**
   * In situations where a method or constructor return a class that implements Closeable, Spotbugs
   * wants to see it closed. When testing bounds and expecting exceptions, this is unnecessary, and
   * trying to work around this in tests results in a lot of boilerplate. This method absorbs all
   * the boilerplate and acts as `assertThrows`.
   *
   * @param expectedType exception type expected
   * @param executable code that returns something Closeable, and expected to throw
   * @param <T> exception type
   * @param <C> return type
   */
  @SneakyThrows
  @SuppressWarnings("try")
  public static <T extends Throwable, C extends Closeable> void assertThrowsClosableResult(
      Class<T> expectedType, ThrowingSupplier<C> executable) {
    try (C result = executable.get()) {
    } catch (Throwable throwable) {
      assertInstanceOf(expectedType, throwable);
      return;
    }
    fail(String.format("Exception of type '%s' was expected. Nothing was thrown", expectedType));
  }

  /**
   * In situations where a read method of a class extending InputStream, spotbugs wants to assert
   * return value is checked (@link https://spotbugs.readthedocs.io/en/stable/bugDescriptions.html).
   *
   * @param expectedType exception type expected
   * @param executable code that returns something, and expected to throw
   * @param expected expected return value, though will never be executed as executable is throwing
   * @param <T> exception type
   * @param <R> return type
   */
  public static <T extends Throwable, R> void assertReadResult(
      Class<T> expectedType, ThrowingSupplier<R> executable, R expected) {
    try {
      R result = executable.get();
      assertEquals(expected, result);
    } catch (Throwable throwable) {
      assertInstanceOf(expectedType, throwable);
      return;
    }
    fail(String.format("Exception of type '%s' was expected. Nothing was thrown", expectedType));
  }
}

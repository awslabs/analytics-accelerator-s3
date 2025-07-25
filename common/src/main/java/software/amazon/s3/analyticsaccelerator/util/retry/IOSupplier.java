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
package software.amazon.s3.analyticsaccelerator.util.retry;

import java.io.IOException;

/**
 * A function that mimics {@link java.util.function.Supplier}, but allows IOException to be thrown
 * and returns T.
 */
@FunctionalInterface
public interface IOSupplier<T> {

  /**
   * Functional representation of the code that takes no parameters and returns a value of type
   * {@link T}. The code is allowed to throw any exception.
   *
   * @return a value of type {@link T}.
   * @throws IOException on error condition.
   */
  T apply() throws IOException;
}

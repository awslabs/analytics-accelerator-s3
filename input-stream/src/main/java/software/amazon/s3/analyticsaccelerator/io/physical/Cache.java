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
package software.amazon.s3.analyticsaccelerator.io.physical;

/**
 * A Cache interface defining how the ElastiCache cache should behave, to be used in a shared cache
 * that may be accessed by any node for the duration of a workload
 */
public interface Cache {

  /**
   * Fetches the value from ElastiCache given a key
   *
   * @param key the key to fetch from ElastiCache
   * @return the value associated with the key in ElastiCache
   */
  byte[] get(String key);

  /**
   * Sets the value in ElastiCache for a key, given that key and value
   *
   * @param key the key for which to set the value in ElastiCache
   * @param value the value to set in ElastiCache for the given key
   */
  void set(String key, byte[] value);

  /** Closes the connection to the ElastiCache server */
  void close();
}

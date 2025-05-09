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
package software.amazon.s3.analyticsaccelerator.io.physical.impl;

import io.valkey.ConnectionPoolConfig;
import io.valkey.DefaultJedisClientConfig;
import io.valkey.HostAndPort;
import io.valkey.JedisClientConfig;
import io.valkey.JedisCluster;
import software.amazon.s3.analyticsaccelerator.io.physical.Cache;

/** A Valkey implementation of the Cache frontend */
public class ValkeyCacheImpl implements Cache {
  private final JedisCluster jedisCluster;
  private final int max_attempts;

  /**
   * Construct a new instance of ValkeyCacheImpl.
   *
   * @param endpoint the ElastiCache endpoint of the serverless Valkey cache
   */
  public ValkeyCacheImpl(String endpoint) {

    ConnectionPoolConfig config = new ConnectionPoolConfig();
    config.setMaxTotal(64);
    config.setMaxIdle(64);
    config.setMinIdle(32);

    JedisClientConfig clientConfig = DefaultJedisClientConfig.builder().ssl(true).build();
    this.max_attempts = 5;
    this.jedisCluster =
        new JedisCluster(new HostAndPort(endpoint, 6379), clientConfig, max_attempts, config);
  }

  /**
   * Fetches the value from the Valkey ElastiCache given a key
   *
   * @param key the key to fetch from the Valkey ElastiCache
   * @return the value associated with the key in the Valkey ElastiCache
   */
  @Override
  public byte[] get(byte[] key) {
    return jedisCluster.get(key);
  }

  /**
   * Sets the value in the Valkey ElastiCache for a key, given that key and value
   *
   * @param key the key for which to set the value in the Valkey ElastiCache
   * @param value the value to set in the Valkey ElastiCache for the given key
   */
  @Override
  public void set(byte[] key, byte[] value) {
    jedisCluster.set(key, value);
  }

  /** Closes the connection to the Valkey ElastiCache server */
  @Override
  public void close() {
    if (jedisCluster != null) {
      jedisCluster.close();
    }
  }
}

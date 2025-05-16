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
import io.valkey.exceptions.JedisException;
import java.nio.charset.StandardCharsets;
import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.io.physical.Cache;

/** A Valkey implementation of the Cache frontend */
public class ValkeyCacheImpl implements Cache {
  private static final int MAX_ATTEMPTS = 5;
  private static final int MAX_POOL_CONNECTIONS = 32;
  private static final int MIN_IDLE_POOL_CONNECTIONS = 16;

  private final JedisCluster jedisCluster;

  /**
   * Construct a new instance of ValkeyCacheImpl.
   *
   * @param endpoint the ElastiCache endpoint of the serverless Valkey cache
   */
  public ValkeyCacheImpl(@NonNull String endpoint) throws JedisException {

    ConnectionPoolConfig config = new ConnectionPoolConfig();
    config.setMaxTotal(MAX_POOL_CONNECTIONS);
    config.setMaxIdle(MAX_POOL_CONNECTIONS);
    config.setMinIdle(MIN_IDLE_POOL_CONNECTIONS);

    JedisClientConfig clientConfig = DefaultJedisClientConfig.builder().ssl(true).build();
    this.jedisCluster =
        new JedisCluster(new HostAndPort(endpoint, 6379), clientConfig, MAX_ATTEMPTS, config);
  }

  /**
   * Construct a new instance of ValkeyCacheImpl for testing purposes that accepts pre-configured
   * JedisCluster.
   *
   * @param jedisCluster the JedisCluster (mock) instance to use.
   */
  public ValkeyCacheImpl(@NonNull JedisCluster jedisCluster) {
    this.jedisCluster = jedisCluster;
  }

  /**
   * Fetches the value from the Valkey ElastiCache given a key
   *
   * @param key the key to fetch from the Valkey ElastiCache
   * @return the value associated with the key in the Valkey ElastiCache
   */
  @Override
  public byte[] get(String key) {
    return jedisCluster.get(key.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Sets the value in the Valkey ElastiCache for a key, given that key and value
   *
   * @param key the key for which to set the value in the Valkey ElastiCache
   * @param value the value to set in the Valkey ElastiCache for the given key
   */
  @Override
  public void set(String key, byte[] value) {
    jedisCluster.set(key.getBytes(StandardCharsets.UTF_8), value);
  }

  /** Closes the connection to the Valkey ElastiCache server */
  @Override
  public void close() {
    if (jedisCluster != null) {
      jedisCluster.close();
    }
  }
}

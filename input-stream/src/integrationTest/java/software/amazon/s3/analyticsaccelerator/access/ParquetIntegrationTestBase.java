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
package software.amazon.s3.analyticsaccelerator.access;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetColumnPrefetchStore;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ColumnMappers;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * Base class for Parquet-specific integration tests. Provides common utilities and helper methods
 * for testing Parquet optimizations including column tracking, dictionary prefetching, footer
 * caching, and multi-row group handling.
 */
public abstract class ParquetIntegrationTestBase extends IntegrationTestBase {

  protected static final int ASYNC_TRACKING_WAIT_MS = 200;
  protected static final int DICTIONARY_READ_SIZE = 100;
  protected static final int COLUMN_READ_BUFFER = 1000;
  protected static final int FOOTER_PREFETCH_SIZE = 100;

  static List<S3ClientKind> clientKinds() {
    return Collections.singletonList(S3ClientKind.SDK_V2_JAVA_SYNC);
  }
  /**
   * Waits for column mappers to be populated after footer parsing. Uses polling with timeout for
   * robust async handling.
   *
   * @param reader the stream reader
   * @param stream the input stream
   * @return populated ColumnMappers
   * @throws Exception if footer parsing fails or times out
   */
  protected ColumnMappers waitForColumnMappers(
      S3AALClientStreamReader reader, S3SeekableInputStream stream) throws Exception {
    int bytesRead = stream.read(new byte[1]);
    assertTrue(bytesRead > 0, "Should read at least one byte to trigger footer parsing");

    ParquetColumnPrefetchStore store = getStore(reader);
    S3URI uri = getS3URI(stream);

    CompletableFuture<ColumnMappers> task =
        CompletableFuture.supplyAsync(
            () -> {
              ColumnMappers mappers;
              while ((mappers = store.getColumnMappers(uri)) == null) {
                try {
                  Thread.sleep(50);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  return null;
                }
              }
              return mappers;
            });

    ColumnMappers mappers = task.get(2, TimeUnit.SECONDS);
    assertNotNull(mappers, "ColumnMappers should be populated after footer parsing");
    return mappers;
  }

  /**
   * Retrieves the ParquetColumnPrefetchStore from the stream reader using reflection.
   *
   * @param reader the stream reader
   * @return the ParquetColumnPrefetchStore instance
   * @throws Exception if reflection fails
   */
  protected ParquetColumnPrefetchStore getStore(S3AALClientStreamReader reader) throws Exception {
    Field field =
        reader
            .getS3SeekableInputStreamFactory()
            .getClass()
            .getDeclaredField("parquetColumnPrefetchStore");
    java.security.AccessController.doPrivileged(
        (java.security.PrivilegedAction<Void>)
            () -> {
              field.setAccessible(true);
              return null;
            });
    return (ParquetColumnPrefetchStore) field.get(reader.getS3SeekableInputStreamFactory());
  }

  /**
   * Retrieves the S3URI from a stream using reflection.
   *
   * @param stream the input stream
   * @return the S3URI
   * @throws Exception if reflection fails
   */
  protected S3URI getS3URI(S3SeekableInputStream stream) throws Exception {
    Field field = stream.getClass().getDeclaredField("s3URI");
    java.security.AccessController.doPrivileged(
        (java.security.PrivilegedAction<Void>)
            () -> {
              field.setAccessible(true);
              return null;
            });
    return (S3URI) field.get(stream);
  }

  /**
   * Creates a standard Parquet test configuration.
   *
   * @param enableSmallObjectPrefetch whether to enable small object prefetching
   * @return configured S3SeekableInputStreamConfiguration
   */
  protected S3SeekableInputStreamConfiguration createParquetConfig(
      boolean enableSmallObjectPrefetch) {
    Map<String, String> config = new HashMap<>();
    config.put(
        "physicalio.small.objects.prefetching.enabled", String.valueOf(enableSmallObjectPrefetch));
    config.put("logicalio.prefetching.mode", "ALL");
    return S3SeekableInputStreamConfiguration.fromConfiguration(
        new ConnectorConfiguration(config, ""));
  }

  /**
   * Creates a configuration for footer caching tests.
   *
   * @param footerSize the footer prefetch size in bytes
   * @return configured S3SeekableInputStreamConfiguration
   */
  protected S3SeekableInputStreamConfiguration createFooterConfig(int footerSize) {
    Map<String, String> config = new HashMap<>();
    config.put("physicalio.small.objects.prefetching.enabled", "false");
    config.put("logicalio.prefetch.file.metadata.size", String.valueOf(footerSize));
    config.put("logicalio.prefetch.file.page.index.size", String.valueOf(footerSize));
    return S3SeekableInputStreamConfiguration.fromConfiguration(
        new ConnectorConfiguration(config, ""));
  }

  /**
   * Calculates the appropriate read size for a column to trigger column tracking.
   *
   * @param dictionarySize the size of the dictionary page
   * @return the read size that exceeds dictionary size
   */
  protected int calculateColumnReadSize(long dictionarySize) {
    return (int) Math.max(dictionarySize + COLUMN_READ_BUFFER, 10000);
  }
}

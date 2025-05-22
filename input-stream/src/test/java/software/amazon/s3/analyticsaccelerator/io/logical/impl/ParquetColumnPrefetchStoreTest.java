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
package software.amazon.s3.analyticsaccelerator.io.logical.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ColumnMappers;
import software.amazon.s3.analyticsaccelerator.io.logical.parquet.ColumnMetadata;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class ParquetColumnPrefetchStoreTest {

  @Test
  void testConstructor() {
    assertNotNull(new ParquetColumnPrefetchStore(mock(LogicalIOConfiguration.class)));
  }

  @Test
  void addRecentColumn() {
    StringBuilder concatedColumnString =
        new StringBuilder().append("sk_test").append("sk_test_2").append("sk_test_3");
    int schemaHash = concatedColumnString.toString().hashCode();
    Map<S3URI, ColumnMappers> columnMappersStore = new HashMap<>();
    Map<S3URI, List<Integer>> rowGroupsPrefetched = new HashMap<>();
    Map<S3URI, List<Integer>> dictionaryRowGroupsPrefetched = new HashMap<>();

    ColumnMetadata sk_test = new ColumnMetadata(0, "sk_test", 0, 0, 0, 500, schemaHash);
    ColumnMetadata sk_test2 = new ColumnMetadata(0, "sk_test2", 0, 0, 0, 500, schemaHash);
    ColumnMetadata sk_test3 = new ColumnMetadata(0, "sk_test3", 0, 0, 0, 500, schemaHash);

    Map<Integer, LinkedList<String>> recentlyReadColumnsPerSchema = new HashMap<>();
    Map<Integer, LinkedList<String>> recentlyReadDictionariesPerSchema = new HashMap<>();

    ParquetColumnPrefetchStore parquetColumnPrefetchStore =
        new ParquetColumnPrefetchStore(
            LogicalIOConfiguration.builder().maxColumnAccessCountStoreSize(3).build(),
            columnMappersStore,
            recentlyReadColumnsPerSchema,
            recentlyReadDictionariesPerSchema,
            rowGroupsPrefetched,
            dictionaryRowGroupsPrefetched);

    parquetColumnPrefetchStore.addRecentColumn(sk_test);
    parquetColumnPrefetchStore.addRecentColumn(sk_test2);
    parquetColumnPrefetchStore.addRecentColumn(sk_test);
    parquetColumnPrefetchStore.addRecentColumn(sk_test2);
    parquetColumnPrefetchStore.addRecentColumn(sk_test3);
    parquetColumnPrefetchStore.addRecentColumn(sk_test3);

    assertEquals(recentlyReadColumnsPerSchema.get(schemaHash).size(), 3);

    // We should only have the last 3 recently read columns in the list
    List<String> expectedColumns = new ArrayList<>();
    expectedColumns.add("sk_test2");
    expectedColumns.add("sk_test3");
    expectedColumns.add("sk_test3");

    assertEquals(recentlyReadColumnsPerSchema.get(schemaHash), expectedColumns);

    // We should only have unique columns in the list
    Set<String> expectedUniqueColumns = new HashSet<>();
    expectedUniqueColumns.add("sk_test2");
    expectedUniqueColumns.add("sk_test3");

    assertEquals(
        parquetColumnPrefetchStore.getUniqueRecentColumnsForSchema(schemaHash),
        expectedUniqueColumns);
  }

  @Test
  public void isRowGroupPrefetched() {
    Map<S3URI, List<Integer>> columnRowGroupsPrefetched = new HashMap<>();
    Map<S3URI, List<Integer>> dictionaryRowGroupsPrefetched = new HashMap<>();

    ParquetColumnPrefetchStore parquetColumnPrefetchStore =
        new ParquetColumnPrefetchStore(
            LogicalIOConfiguration.builder().maxColumnAccessCountStoreSize(3).build(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            columnRowGroupsPrefetched,
            dictionaryRowGroupsPrefetched);

    parquetColumnPrefetchStore.storeColumnPrefetchedRowGroupIndex(S3URI.of("test", "key"), 0);
    parquetColumnPrefetchStore.storeColumnPrefetchedRowGroupIndex(S3URI.of("test", "key"), 1);

    parquetColumnPrefetchStore.storeDictionaryPrefetchedRowGroupIndex(S3URI.of("test", "key"), 0);
    parquetColumnPrefetchStore.storeDictionaryPrefetchedRowGroupIndex(S3URI.of("test", "key"), 1);

    assertEquals(
        parquetColumnPrefetchStore.isColumnRowGroupPrefetched(S3URI.of("test", "key"), 0), true);
    assertEquals(
        parquetColumnPrefetchStore.isDictionaryRowGroupPrefetched(S3URI.of("test", "key"), 0),
        true);
    assertEquals(
        parquetColumnPrefetchStore.isColumnRowGroupPrefetched(S3URI.of("test", "key"), 1), true);
    assertEquals(
        parquetColumnPrefetchStore.isDictionaryRowGroupPrefetched(S3URI.of("test", "key"), 1),
        true);
    assertEquals(
        parquetColumnPrefetchStore.isColumnRowGroupPrefetched(S3URI.of("test", "key"), 3), false);
    assertEquals(
        parquetColumnPrefetchStore.isDictionaryRowGroupPrefetched(S3URI.of("test", "key"), 3),
        false);
    assertEquals(
        parquetColumnPrefetchStore.isColumnRowGroupPrefetched(S3URI.of("test", "key_3"), 0), false);
    assertEquals(
        parquetColumnPrefetchStore.isDictionaryRowGroupPrefetched(S3URI.of("test", "key_3"), 0),
        false);
  }

  @Test
  void addRecentDictionaries() {
    StringBuilder concatedColumnString =
        new StringBuilder().append("sk_test").append("sk_test_2").append("sk_test_3");
    int schemaHash = concatedColumnString.toString().hashCode();
    Map<S3URI, ColumnMappers> columnMappersStore = new HashMap<>();
    Map<S3URI, List<Integer>> rowGroupsPrefetched = new HashMap<>();
    Map<S3URI, List<Integer>> dictionaryRowGroupsPrefetched = new HashMap<>();

    ColumnMetadata sk_test = new ColumnMetadata(0, "sk_test", 0, 0, 0, 500, schemaHash);
    ColumnMetadata sk_test2 = new ColumnMetadata(0, "sk_test2", 0, 0, 0, 500, schemaHash);
    ColumnMetadata sk_test3 = new ColumnMetadata(0, "sk_test3", 0, 0, 0, 500, schemaHash);

    Map<Integer, LinkedList<String>> recentlyReadColumnsPerSchema = new HashMap<>();
    Map<Integer, LinkedList<String>> recentlyReadDictionariesPerSchema = new HashMap<>();

    ParquetColumnPrefetchStore parquetColumnPrefetchStore =
        new ParquetColumnPrefetchStore(
            LogicalIOConfiguration.builder().maxColumnAccessCountStoreSize(3).build(),
            columnMappersStore,
            recentlyReadColumnsPerSchema,
            recentlyReadDictionariesPerSchema,
            rowGroupsPrefetched,
            dictionaryRowGroupsPrefetched);

    parquetColumnPrefetchStore.addRecentDictionary(sk_test);
    parquetColumnPrefetchStore.addRecentDictionary(sk_test2);
    parquetColumnPrefetchStore.addRecentDictionary(sk_test);
    parquetColumnPrefetchStore.addRecentDictionary(sk_test2);
    parquetColumnPrefetchStore.addRecentDictionary(sk_test3);
    parquetColumnPrefetchStore.addRecentDictionary(sk_test3);

    assertEquals(recentlyReadDictionariesPerSchema.get(schemaHash).size(), 3);

    // We should only have the last 3 recently read columns in the list
    List<String> expectedDictionaries = new ArrayList<>();
    expectedDictionaries.add("sk_test2");
    expectedDictionaries.add("sk_test3");
    expectedDictionaries.add("sk_test3");

    assertEquals(recentlyReadDictionariesPerSchema.get(schemaHash), expectedDictionaries);

    // We should only have unique columns in the list
    Set<String> expectedUniqueDictionaries = new HashSet<>();
    expectedUniqueDictionaries.add("sk_test2");
    expectedUniqueDictionaries.add("sk_test3");

    assertEquals(
        parquetColumnPrefetchStore.getUniqueRecentDictionaryForSchema(schemaHash),
        expectedUniqueDictionaries);
  }
}

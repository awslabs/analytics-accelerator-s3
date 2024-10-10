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
package com.amazon.connector.s3.io.logical.parquet;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.logical.impl.ParquetColumnPrefetchStore;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.util.PrefetchMode;
import com.amazon.connector.s3.util.S3URI;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class ParquetPredictivePrefetchingTaskTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  void testConstructor() {
    assertNotNull(
        new ParquetPredictivePrefetchingTask(
            TEST_URI,
            Telemetry.NOOP,
            LogicalIOConfiguration.DEFAULT,
            mock(PhysicalIO.class),
            new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT)));
  }

  @Test
  void testConstructorFailsOnNull() {
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPredictivePrefetchingTask(
                null,
                Telemetry.NOOP,
                LogicalIOConfiguration.DEFAULT,
                mock(PhysicalIO.class),
                new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPredictivePrefetchingTask(
                TEST_URI,
                null,
                LogicalIOConfiguration.DEFAULT,
                mock(PhysicalIO.class),
                new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPredictivePrefetchingTask(
                TEST_URI,
                Telemetry.NOOP,
                null,
                mock(PhysicalIO.class),
                new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPredictivePrefetchingTask(
                TEST_URI,
                Telemetry.NOOP,
                LogicalIOConfiguration.DEFAULT,
                null,
                new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT)));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetPredictivePrefetchingTask(
                TEST_URI,
                Telemetry.NOOP,
                LogicalIOConfiguration.DEFAULT,
                mock(PhysicalIO.class),
                null));
  }

  @Test
  void testAddToRecentColumnList() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetColumnPrefetchStore parquetColumnPrefetchStore = mock(ParquetColumnPrefetchStore.class);

    HashMap<Long, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();
    ColumnMetadata columnMetadata =
        new ColumnMetadata(0, "sk_test", 100, 500, "sk_test".hashCode());
    offsetIndexToColumnMap.put(100L, columnMetadata);
    ColumnMappers columnMappers = new ColumnMappers(offsetIndexToColumnMap, new HashMap<>());
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(
            TEST_URI,
            Telemetry.NOOP,
            LogicalIOConfiguration.DEFAULT,
            physicalIO,
            parquetColumnPrefetchStore);

    when(parquetColumnPrefetchStore.getColumnMappers(TEST_URI)).thenReturn(columnMappers);

    assertTrue(parquetPredictivePrefetchingTask.addToRecentColumnList(100).isPresent());
    verify(parquetColumnPrefetchStore).addRecentColumn(columnMetadata);
  }

  @Test
  void testRowGroupPrefetch() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetColumnPrefetchStore parquetColumnPrefetchStore = mock(ParquetColumnPrefetchStore.class);

    List<ColumnMetadata> columnMetadataList = new ArrayList<>();
    HashMap<Long, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();
    HashMap<String, List<ColumnMetadata>> columnNameToColumnMap = new HashMap<>();
    ColumnMetadata sk_test = new ColumnMetadata(0, "sk_test", 100, 500, "sk_test".hashCode());
    offsetIndexToColumnMap.put(100L, sk_test);
    columnMetadataList.add(sk_test);

    ColumnMetadata sk_test_row_group_1 =
        new ColumnMetadata(1, "sk_test", 800, 500, "sk_test".hashCode());
    offsetIndexToColumnMap.put(800L, sk_test);
    columnMetadataList.add(sk_test_row_group_1);

    columnNameToColumnMap.put("sk_test", columnMetadataList);

    ColumnMappers columnMappers = new ColumnMappers(offsetIndexToColumnMap, columnNameToColumnMap);
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(
            TEST_URI,
            Telemetry.NOOP,
            LogicalIOConfiguration.builder().prefetchingMode(PrefetchMode.ROW_GROUP).build(),
            physicalIO,
            parquetColumnPrefetchStore);

    when(parquetColumnPrefetchStore.isRowGroupPrefetched(TEST_URI, 0)).thenReturn(false);
    when(parquetColumnPrefetchStore.getColumnMappers(TEST_URI)).thenReturn(columnMappers);

    Set<String> recentColumns = new HashSet<>();
    recentColumns.add("sk_test");
    when(parquetColumnPrefetchStore.getUniqueRecentColumnsForSchema("sk_test".hashCode()))
        .thenReturn(recentColumns);

    assertTrue(parquetPredictivePrefetchingTask.addToRecentColumnList(100).isPresent());
    verify(parquetColumnPrefetchStore).addRecentColumn(sk_test);

    // Then: physical IO gets the correct plan. Only recent columns from the current row
    // group are prefetched.
    ArgumentCaptor<IOPlan> ioPlanArgumentCaptor = ArgumentCaptor.forClass(IOPlan.class);
    verify(physicalIO).execute(ioPlanArgumentCaptor.capture());

    IOPlan ioPlan = ioPlanArgumentCaptor.getValue();
    List<Range> expectedRanges = new ArrayList<>();

    expectedRanges.add(new Range(100, 599));
    assertTrue(ioPlan.getPrefetchRanges().containsAll(expectedRanges));
  }

  @Test
  void testAddToRecentColumnListEmptyColumnMappers() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetColumnPrefetchStore parquetColumnPrefetchStore = mock(ParquetColumnPrefetchStore.class);

    when(parquetColumnPrefetchStore.getColumnMappers(TEST_URI)).thenReturn(null);

    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(
            TEST_URI,
            Telemetry.NOOP,
            LogicalIOConfiguration.DEFAULT,
            physicalIO,
            parquetColumnPrefetchStore);

    assertFalse(parquetPredictivePrefetchingTask.addToRecentColumnList(100).isPresent());
    verify(parquetColumnPrefetchStore, times(0)).addRecentColumn(any());
  }

  @Test
  void testPrefetchRecentColumns() throws IOException {
    // Given: prefetching task with some recent columns
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetColumnPrefetchStore parquetColumnPrefetchStore = mock(ParquetColumnPrefetchStore.class);

    StringBuilder columnNames = new StringBuilder();
    columnNames.append("sk_test").append("sk_test_2").append("sk_test_3");
    int schemaHash = columnNames.toString().hashCode();

    HashMap<String, List<ColumnMetadata>> columnNameToColumnMap = new HashMap<>();
    HashMap<Long, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();

    List<ColumnMetadata> sk_testColumnMetadataList = new ArrayList<>();
    ColumnMetadata sk_test1 = new ColumnMetadata(0, "test", 100, 500, schemaHash);
    sk_testColumnMetadataList.add(sk_test1);
    offsetIndexToColumnMap.put(100L, sk_test1);

    // Should not be prefetched as it does not belong to the first row group.
    ColumnMetadata sk_test1_row_group_1 = new ColumnMetadata(1, "test", 1800, 500, schemaHash);
    sk_testColumnMetadataList.add(sk_test1_row_group_1);
    offsetIndexToColumnMap.put(1800L, sk_test1_row_group_1);

    List<ColumnMetadata> sk_test_2ColumnMetadataList = new ArrayList<>();
    ColumnMetadata sk_test2 = new ColumnMetadata(0, "sk_test_2", 600, 500, schemaHash);
    sk_test_2ColumnMetadataList.add(sk_test2);
    offsetIndexToColumnMap.put(600L, sk_test2);

    List<ColumnMetadata> sk_test_3ColumnMetadataList = new ArrayList<>();
    ColumnMetadata sk_test3 =
        new ColumnMetadata(0, "sk_test_3", 1100, 500, getHashCode(columnNames));
    sk_test_3ColumnMetadataList.add(sk_test3);
    offsetIndexToColumnMap.put(1100L, sk_test3);

    columnNameToColumnMap.put("sk_test", sk_testColumnMetadataList);
    columnNameToColumnMap.put("sk_test_2", sk_test_2ColumnMetadataList);
    columnNameToColumnMap.put("sk_test_3", sk_test_3ColumnMetadataList);

    Set<String> recentColumns = new HashSet<>();
    recentColumns.add("sk_test");
    recentColumns.add("sk_test_2");
    recentColumns.add("sk_test_3");
    when(parquetColumnPrefetchStore.getUniqueRecentColumnsForSchema(schemaHash))
        .thenReturn(recentColumns);

    // When: recent columns get prefetched
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(
            TEST_URI,
            Telemetry.NOOP,
            LogicalIOConfiguration.DEFAULT,
            physicalIO,
            parquetColumnPrefetchStore);
    parquetPredictivePrefetchingTask.prefetchRecentColumns(
        new ColumnMappers(offsetIndexToColumnMap, columnNameToColumnMap),
        ParquetUtils.constructRowGroupsToPrefetch());

    // Then: physical IO gets the correct plan
    ArgumentCaptor<IOPlan> ioPlanArgumentCaptor = ArgumentCaptor.forClass(IOPlan.class);
    verify(physicalIO).execute(ioPlanArgumentCaptor.capture());

    IOPlan ioPlan = ioPlanArgumentCaptor.getValue();
    List<Range> expectedRanges = new ArrayList<>();

    expectedRanges.add(new Range(100, 599));
    expectedRanges.add(new Range(600, 1099));
    expectedRanges.add(new Range(1100, 1599));
    assertTrue(ioPlan.getPrefetchRanges().containsAll(expectedRanges));
  }

  @Test
  void testExceptionRemappedToCompletionException() throws IOException {
    // Given: a task performing predictive prefetching
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(
            TEST_URI,
            Telemetry.NOOP,
            LogicalIOConfiguration.DEFAULT,
            physicalIO,
            new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT));

    // When: the underlying PhysicalIO always throws
    doThrow(new IOException("Error in prefetch")).when(physicalIO).execute(any(IOPlan.class));

    // Then: exceptions are wrapped by CompletionExceptions
    assertThrows(
        CompletionException.class,
        () ->
            parquetPredictivePrefetchingTask.prefetchRecentColumns(
                new ColumnMappers(new HashMap<>(), new HashMap<>()), Collections.emptyList()));
  }

  private int getHashCode(StringBuilder stringToHash) {
    return stringToHash.toString().hashCode();
  }
}

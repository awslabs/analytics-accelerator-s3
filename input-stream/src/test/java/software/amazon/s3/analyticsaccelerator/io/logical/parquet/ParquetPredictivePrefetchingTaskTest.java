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
package software.amazon.s3.analyticsaccelerator.io.logical.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_KB;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.s3.analyticsaccelerator.common.telemetry.Telemetry;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetColumnPrefetchStore;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIO;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanExecution;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanState;
import software.amazon.s3.analyticsaccelerator.request.Range;
import software.amazon.s3.analyticsaccelerator.request.ReadMode;
import software.amazon.s3.analyticsaccelerator.util.PrefetchMode;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

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
        new ColumnMetadata(0, "sk_test", 200, 100, 100, 500, "sk_test".hashCode());
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

    assertEquals(1, parquetPredictivePrefetchingTask.addToRecentColumnList(100, 400).size());
    verify(parquetColumnPrefetchStore).addRecentColumn(columnMetadata);
  }

  @Test
  void testRowGroupPrefetch() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetColumnPrefetchStore parquetColumnPrefetchStore = mock(ParquetColumnPrefetchStore.class);

    List<ColumnMetadata> columnMetadataList = new ArrayList<>();
    HashMap<Long, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();
    HashMap<String, List<ColumnMetadata>> columnNameToColumnMap = new HashMap<>();

    ColumnMetadata sk_test =
        new ColumnMetadata(0, "sk_test", 200, 100, 100, 500, "sk_test".hashCode());
    offsetIndexToColumnMap.put(100L, sk_test);
    columnMetadataList.add(sk_test);

    ColumnMetadata sk_test_row_group_1 =
        new ColumnMetadata(1, "sk_test", 900, 800, 800, 500, "sk_test".hashCode());
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

    when(parquetColumnPrefetchStore.isColumnRowGroupPrefetched(TEST_URI, 0)).thenReturn(false);
    when(parquetColumnPrefetchStore.getColumnMappers(TEST_URI)).thenReturn(columnMappers);

    Set<String> recentColumns = new HashSet<>();
    recentColumns.add("sk_test");
    when(parquetColumnPrefetchStore.getUniqueRecentColumnsForSchema("sk_test".hashCode()))
        .thenReturn(recentColumns);

    assertEquals(1, parquetPredictivePrefetchingTask.addToRecentColumnList(100, 200).size());
    verify(parquetColumnPrefetchStore).addRecentColumn(sk_test);

    // Then: physical IO gets the correct plan. Only recent columns from the current row
    // group are prefetched.
    ArgumentCaptor<IOPlan> ioPlanArgumentCaptor = ArgumentCaptor.forClass(IOPlan.class);
    ArgumentCaptor<ReadMode> readModeCaptor = ArgumentCaptor.forClass(ReadMode.class);
    verify(physicalIO, times(2)).execute(ioPlanArgumentCaptor.capture(), readModeCaptor.capture());

    IOPlan ioPlan = ioPlanArgumentCaptor.getValue();
    List<Range> expectedRanges = new ArrayList<>();

    expectedRanges.add(new Range(100, 599));
    assertTrue(ioPlan.getPrefetchRanges().containsAll(expectedRanges));
    assertEquals(readModeCaptor.getValue(), ReadMode.COLUMN_PREFETCH);
  }

  @Test
  void testRowGroupPrefetchForOnlyDictionary() throws IOException {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetColumnPrefetchStore parquetColumnPrefetchStore = mock(ParquetColumnPrefetchStore.class);

    List<ColumnMetadata> columnMetadataList = new ArrayList<>();
    HashMap<Long, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();
    HashMap<String, List<ColumnMetadata>> columnNameToColumnMap = new HashMap<>();

    ColumnMetadata sk_test =
        new ColumnMetadata(0, "sk_test", 200, 100, 100, 500, "sk_test".hashCode());
    offsetIndexToColumnMap.put(100L, sk_test);
    columnMetadataList.add(sk_test);

    ColumnMetadata sk_test_row_group_1 =
        new ColumnMetadata(1, "sk_test", 900, 800, 800, 500, "sk_test".hashCode());
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

    when(parquetColumnPrefetchStore.isDictionaryRowGroupPrefetched(TEST_URI, 0)).thenReturn(false);
    when(parquetColumnPrefetchStore.getColumnMappers(TEST_URI)).thenReturn(columnMappers);

    Set<String> recentDictionaries = new HashSet<>();
    recentDictionaries.add("sk_test");
    when(parquetColumnPrefetchStore.getUniqueRecentDictionaryForSchema("sk_test".hashCode()))
        .thenReturn(recentDictionaries);

    assertEquals(1, parquetPredictivePrefetchingTask.addToRecentColumnList(100, 50).size());
    verify(parquetColumnPrefetchStore).addRecentDictionary(sk_test);

    // Then: physical IO gets the correct plan. Only recent columns from the current row
    // group are prefetched.
    ArgumentCaptor<IOPlan> ioPlanArgumentCaptor = ArgumentCaptor.forClass(IOPlan.class);
    ArgumentCaptor<ReadMode> readModeCaptor = ArgumentCaptor.forClass(ReadMode.class);
    verify(physicalIO, times(2)).execute(ioPlanArgumentCaptor.capture(), readModeCaptor.capture());

    IOPlan ioPlan = ioPlanArgumentCaptor.getAllValues().get(0);
    List<Range> expectedRanges = new ArrayList<>();

    List<ReadMode> readModes = readModeCaptor.getAllValues();

    expectedRanges.add(new Range(100, 199));
    assertTrue(ioPlan.getPrefetchRanges().containsAll(expectedRanges));
    assertEquals(readModes.get(0), ReadMode.DICTIONARY_PREFETCH);
    assertEquals(readModes.get(1), ReadMode.COLUMN_PREFETCH);
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

    assertTrue(parquetPredictivePrefetchingTask.addToRecentColumnList(100, 0).isEmpty());
    verify(parquetColumnPrefetchStore, times(0)).addRecentColumn(any());
  }

  @Test
  void testAddToRecentColumnListAdjacentColumns() {
    PhysicalIO physicalIO = mock(PhysicalIO.class);
    ParquetColumnPrefetchStore parquetColumnPrefetchStore = mock(ParquetColumnPrefetchStore.class);

    StringBuilder columnNames = new StringBuilder();
    columnNames.append("sk_test").append("sk_test_2").append("sk_test_3");
    int schemaHash = columnNames.toString().hashCode();

    HashMap<String, List<ColumnMetadata>> columnNameToColumnMap = new HashMap<>();
    HashMap<Long, ColumnMetadata> offsetIndexToColumnMap = new HashMap<>();

    List<ColumnMetadata> sk_testColumnMetadataList = new ArrayList<>();
    ColumnMetadata sk_test1 =
        new ColumnMetadata(
            0, "sk_test_1", 200 * ONE_KB, 100 * ONE_KB, 100 * ONE_KB, 600 * ONE_KB, schemaHash);
    sk_testColumnMetadataList.add(sk_test1);
    offsetIndexToColumnMap.put(100L * ONE_KB, sk_test1);

    List<ColumnMetadata> sk_test_2ColumnMetadataList = new ArrayList<>();
    ColumnMetadata sk_test2 =
        new ColumnMetadata(
            0, "sk_test_2", 800 * ONE_KB, 700 * ONE_KB, 700 * ONE_KB, 800 * ONE_KB, schemaHash);
    sk_test_2ColumnMetadataList.add(sk_test2);
    offsetIndexToColumnMap.put(700L * ONE_KB, sk_test2);

    List<ColumnMetadata> sk_test_3ColumnMetadataList = new ArrayList<>();
    ColumnMetadata sk_test3 =
        new ColumnMetadata(
            0, "sk_test_3", 1650 * ONE_KB, 1500 * ONE_KB, 1500 * ONE_KB, 200 * ONE_KB, schemaHash);
    sk_test_3ColumnMetadataList.add(sk_test3);
    offsetIndexToColumnMap.put(1500L * ONE_KB, sk_test3);

    List<ColumnMetadata> sk_test_4ColumnMetadataList = new ArrayList<>();
    ColumnMetadata sk_test4 =
        new ColumnMetadata(
            0, "sk_test_4", 1800 * ONE_KB, 1700 * ONE_KB, 1700 * ONE_KB, 800 * ONE_KB, schemaHash);
    sk_test_4ColumnMetadataList.add(sk_test4);
    offsetIndexToColumnMap.put(1700L * ONE_KB, sk_test4);

    columnNameToColumnMap.put("sk_test_1", sk_testColumnMetadataList);
    columnNameToColumnMap.put("sk_test_2", sk_test_2ColumnMetadataList);
    columnNameToColumnMap.put("sk_test_3", sk_test_3ColumnMetadataList);
    columnNameToColumnMap.put("sk_test_4", sk_test_4ColumnMetadataList);

    when(parquetColumnPrefetchStore.getColumnMappers(any(S3URI.class)))
        .thenReturn(new ColumnMappers(offsetIndexToColumnMap, columnNameToColumnMap));

    ParquetPredictivePrefetchingTask parquetPredictivePrefetchingTask =
        new ParquetPredictivePrefetchingTask(
            TEST_URI,
            Telemetry.NOOP,
            LogicalIOConfiguration.DEFAULT,
            physicalIO,
            parquetColumnPrefetchStore);

    // Test the case were the read is at starting position of a column x, but has a longer length
    // than the size of x,
    // so may contain multiple columns.
    List<ColumnMetadata> addedColumns =
        parquetPredictivePrefetchingTask.addToRecentColumnList(100 * ONE_KB, 1600 * ONE_KB);
    List<ColumnMetadata> expectedColumns = new ArrayList<>();
    expectedColumns.add(sk_test1);
    expectedColumns.add(sk_test2);
    expectedColumns.add(sk_test3);

    verify(parquetColumnPrefetchStore, times(3)).addRecentColumn(any());
    assertTrue(expectedColumns.containsAll(addedColumns));
    assertEquals(3, addedColumns.size());

    //     Test the case where the read begins in the middle of the column. Eg 1900 is within the
    // column
    //     boundary of sk_test_4
    List<ColumnMetadata> addedColumns2 =
        parquetPredictivePrefetchingTask.addToRecentColumnList(1900 * ONE_KB, 600 * ONE_KB);
    List<ColumnMetadata> expectedColumns2 = new ArrayList<>();
    expectedColumns2.add(sk_test4);
    verify(parquetColumnPrefetchStore, times(1)).addRecentColumn(sk_test4);
    assertEquals(1, addedColumns2.size());
    assertTrue(expectedColumns2.containsAll(addedColumns2));

    // 800 is in the boundary of sk_test_2
    List<ColumnMetadata> addedColumns3 =
        parquetPredictivePrefetchingTask.addToRecentColumnList(800 * ONE_KB, 1000 * ONE_KB);
    List<ColumnMetadata> expectedColumns3 = new ArrayList<>();
    expectedColumns3.add(sk_test2);
    assertEquals(1, addedColumns3.size());
    assertTrue(expectedColumns3.containsAll(addedColumns3));
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
    ColumnMetadata sk_test1 = new ColumnMetadata(0, "test", 200, 100, 100, 500, schemaHash);
    sk_testColumnMetadataList.add(sk_test1);
    offsetIndexToColumnMap.put(100L, sk_test1);

    // Should not be prefetched as it does not belong to the first row group.
    ColumnMetadata sk_test1_row_group_1 =
        new ColumnMetadata(1, "test", 1900, 1800, 1800, 500, schemaHash);
    sk_testColumnMetadataList.add(sk_test1_row_group_1);
    offsetIndexToColumnMap.put(1800L, sk_test1_row_group_1);

    List<ColumnMetadata> sk_test_2ColumnMetadataList = new ArrayList<>();
    ColumnMetadata sk_test2 = new ColumnMetadata(0, "sk_test_2", 700, 600, 600, 500, schemaHash);
    sk_test_2ColumnMetadataList.add(sk_test2);
    offsetIndexToColumnMap.put(600L, sk_test2);

    List<ColumnMetadata> sk_test_3ColumnMetadataList = new ArrayList<>();
    ColumnMetadata sk_test3 =
        new ColumnMetadata(0, "sk_test_3", 1400, 1300, 1300, 500, getHashCode(columnNames));
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
        ParquetUtils.constructRowGroupsToPrefetch(),
        false);

    // Then: physical IO gets the correct plan
    ArgumentCaptor<IOPlan> ioPlanArgumentCaptor = ArgumentCaptor.forClass(IOPlan.class);
    ArgumentCaptor<ReadMode> readModeCaptor = ArgumentCaptor.forClass(ReadMode.class);
    verify(physicalIO, times(2)).execute(ioPlanArgumentCaptor.capture(), readModeCaptor.capture());

    IOPlan ioPlan = ioPlanArgumentCaptor.getValue();
    List<Range> expectedRanges = new ArrayList<>();

    // first two columns are consecutive, and so get merged
    expectedRanges.add(new Range(100, 1099));
    expectedRanges.add(new Range(1300, 1799));
    assertTrue(ioPlan.getPrefetchRanges().containsAll(expectedRanges));
    assertEquals(readModeCaptor.getValue(), ReadMode.COLUMN_PREFETCH);
  }

  @Test
  void testExceptionInPrefetchingIsSwallowed() throws IOException {
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
    doThrow(new IOException("Error in prefetch"))
        .when(physicalIO)
        .execute(any(IOPlan.class), any(ReadMode.class));

    assertEquals(
        IOPlanExecution.builder().state(IOPlanState.SKIPPED).build(),
        parquetPredictivePrefetchingTask.prefetchRecentColumns(
            new ColumnMappers(new HashMap<>(), new HashMap<>()), Collections.emptyList(), false));
  }

  private int getHashCode(StringBuilder stringToHash) {
    return stringToHash.toString().hashCode();
  }
}

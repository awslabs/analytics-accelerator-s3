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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.utils.ImmutableMap;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetColumnPrefetchStore;
import software.amazon.s3.analyticsaccelerator.util.DefaultRequestCallbackImpl;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@SuppressFBWarnings(
    value = "NP_NONNULL_PARAM_VIOLATION",
    justification = "We mean to pass nulls to checks")
public class ParquetMetadataParsingTaskTest {
  private static final S3URI TEST_URI = S3URI.of("foo", "bar");

  @Test
  void testConstructor() {
    assertNotNull(
        new ParquetMetadataParsingTask(
            TEST_URI,
            new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT),
            new DefaultRequestCallbackImpl()));
  }

  @Test
  void testConstructorFailsOnNull() {
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetMetadataParsingTask(
                null,
                new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT),
                new DefaultRequestCallbackImpl()));
    assertThrows(
        NullPointerException.class,
        () -> new ParquetMetadataParsingTask(TEST_URI, null, new DefaultRequestCallbackImpl()));
    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetMetadataParsingTask(
                null,
                new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT),
                mock(ParquetParser.class),
                new DefaultRequestCallbackImpl()));

    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetMetadataParsingTask(
                TEST_URI, null, mock(ParquetParser.class), new DefaultRequestCallbackImpl()));

    assertThrows(
        NullPointerException.class,
        () ->
            new ParquetMetadataParsingTask(
                TEST_URI, new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT), null));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "src/test/resources/call_center_file_metadata.ser",
        "src/test/resources/nested_data_metadata.ser"
      })
  void testColumnMapCreation(String fileMetadata) throws IOException, ClassNotFoundException {

    FileMetaData fileMetaData = getFileMetadata(fileMetadata);
    ColumnMappers columnMappers = getColumnMappers(fileMetaData);

    assertEquals(
        fileMetaData.getRow_groups().get(0).getColumns().size(),
        columnMappers.getOffsetIndexToColumnMap().size());

    for (ColumnChunk columnChunk : fileMetaData.getRow_groups().get(0).getColumns()) {
      Long key;

      // If the column has a dictionary, key should be equal to dictionary_page_offset as this is
      // where reads for this column start.
      if (columnChunk.getMeta_data().getDictionary_page_offset() != 0) {
        key = columnChunk.getMeta_data().getDictionary_page_offset();
      } else {
        key = columnChunk.getFile_offset();
      }

      assertTrue(columnMappers.getOffsetIndexToColumnMap().containsKey(key));
      assertEquals(0, columnMappers.getOffsetIndexToColumnMap().get(key).getRowGroupIndex());
      assertEquals(
          String.join(".", columnChunk.getMeta_data().getPath_in_schema()),
          columnMappers.getOffsetIndexToColumnMap().get(key).getColumnName());
      assertEquals(
          columnChunk.getMeta_data().getTotal_compressed_size(),
          columnMappers.getOffsetIndexToColumnMap().get(key).getCompressedSize());
    }
  }

  @ParameterizedTest
  @MethodSource("arguments")
  void testColumnMapCreationMultiRowGroup(
      String filename, Map<String, Integer> expectedColToRowGroup, int expectedColumns)
      throws IOException, ClassNotFoundException {
    // Deserialize fileMetaData object
    FileMetaData fileMetaData = getFileMetadata(filename);
    ColumnMappers columnMappers = getColumnMappers(fileMetaData);
    Map<String, List<ColumnMetadata>> columnNameToColumnMap =
        columnMappers.getColumnNameToColumnMap();

    // parquet file "multi_row_group.parquet" in resources has 2 columns and 3 row groups. So the
    // map
    // should have two entries (one for each column) of size 3 (one entry for each occurrence of the
    // column)
    assertEquals(expectedColumns, columnNameToColumnMap.size());
    expectedColToRowGroup.forEach(
        (col, rowGroup) -> {
          assertEquals(rowGroup, columnNameToColumnMap.get(col).size());
        });

    int rowGroupIndex = 0;
    for (RowGroup rowGroup : fileMetaData.getRow_groups()) {
      assertEquals(rowGroup.getColumns().size(), columnNameToColumnMap.size());
      for (ColumnChunk columnChunk : rowGroup.getColumns()) {
        List<ColumnMetadata> columnMetadataList =
            columnNameToColumnMap.get(
                String.join(".", columnChunk.getMeta_data().getPath_in_schema()));
        ColumnMetadata columnMetadata = columnMetadataList.get(rowGroupIndex);

        assertEquals(
            columnMetadata.getColumnName(),
            String.join(".", columnChunk.getMeta_data().getPath_in_schema()));

        // If the column has a dictionary, start pos should be equal to dictionary_page_offset as
        // this is
        // where reads for this column start.
        long startPos;
        if (columnChunk.getMeta_data().getDictionary_page_offset() != 0) {
          startPos = columnChunk.getMeta_data().getDictionary_page_offset();
        } else {
          startPos = columnChunk.getFile_offset();
        }

        assertEquals(startPos, columnMetadata.getStartPos());
        assertEquals(rowGroupIndex, columnMetadata.getRowGroupIndex());
      }
      rowGroupIndex++;
    }
  }

  private static Stream<Arguments> arguments() {
    return Stream.of(
        Arguments.of(
            "src/test/resources/multi_row_group.ser", ImmutableMap.of("n_legs", 3, "animal", 3), 2),
        Arguments.of(
            "src/test/resources/nested_data_mrg_metadata.ser",
            ImmutableMap.of(
                "address.city",
                3,
                "address.zip",
                3,
                "phone_numbers.list.element.type",
                3,
                "phone_numbers.list.element.number",
                3),
            8));
  }

  @Test
  void testColumnMapCreationNestedSchema() throws IOException, ClassNotFoundException {
    // Deserialize fileMetaData object
    FileMetaData fileMetaData = getFileMetadata("src/test/resources/nested_data_metadata.ser");
    ColumnMappers columnMappers = getColumnMappers(fileMetaData);

    Map<String, List<ColumnMetadata>> columnNameToColumnMap =
        columnMappers.getColumnNameToColumnMap();
    assertEquals(8, columnNameToColumnMap.size());

    // The underlying data is nested for address and phone number.
    // Eg:   {
    //        'name': 'John Doe',
    //        'age': 30,
    //        'address': {'street': '123 Main St', 'city': 'Anytown', 'state': 'CA', 'zip': 12345},
    //        'phone_numbers': [{'type': 'home', 'number': '555-1234'}, {'type': 'work', 'number':
    // '555-5678'}]
    //    },
    //    {
    //        'name': 'Jane Smith',
    //        'age': 25,
    //        'address': {'street': '456 Maple Ave', 'city': 'Othertown', 'state': 'NY', 'zip':
    // 67890},
    //        'phone_numbers': [{'type': 'home', 'number': '555-8765'}]
    //    }

    // Check the path of the nested columns
    assertTrue(columnNameToColumnMap.containsKey("address.street"));
    assertTrue(columnNameToColumnMap.containsKey("address.zip"));
    assertTrue(columnNameToColumnMap.containsKey("phone_numbers.list.element.type"));
    assertTrue(columnNameToColumnMap.containsKey("phone_numbers.list.element.number"));
  }

  @Test
  void testParsingExceptionsRemappedToCompletionException() throws IOException {
    ParquetParser mockedParquetParser = mock(ParquetParser.class);
    when(mockedParquetParser.parseParquetFooter(any(ByteBuffer.class), anyInt(), any(S3URI.class)))
        .thenThrow(new IOException("can not read FileMetaData"));

    ParquetMetadataParsingTask parquetMetadataParsingTask =
        new ParquetMetadataParsingTask(
            TEST_URI,
            new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT),
            mockedParquetParser,
            new DefaultRequestCallbackImpl());
    CompletableFuture<ColumnMappers> parquetMetadataTaskFuture =
        CompletableFuture.supplyAsync(
            () ->
                parquetMetadataParsingTask.storeColumnMappers(
                    new FileTail(ByteBuffer.allocate(0), 0)));

    assertThrows(CompletionException.class, parquetMetadataTaskFuture::join);
  }

  private FileMetaData getFileMetadata(String filePath) throws IOException, ClassNotFoundException {
    // Deserialize fileMetaData object
    try (FileInputStream fileInStream = new FileInputStream(filePath)) {
      try (ObjectInputStream ois = new ObjectInputStream(fileInStream)) {
        return (FileMetaData) ois.readObject();
      }
    }
  }

  private ColumnMappers getColumnMappers(FileMetaData fileMetaData) throws IOException {
    ParquetParser mockedParquetParser = mock(ParquetParser.class);
    when(mockedParquetParser.parseParquetFooter(any(ByteBuffer.class), anyInt(), any(S3URI.class)))
        .thenReturn(fileMetaData);

    ParquetMetadataParsingTask parquetMetadataParsingTask =
        new ParquetMetadataParsingTask(
            TEST_URI,
            new ParquetColumnPrefetchStore(LogicalIOConfiguration.DEFAULT),
            mockedParquetParser,
            new DefaultRequestCallbackImpl());

    return parquetMetadataParsingTask.storeColumnMappers(new FileTail(ByteBuffer.allocate(0), 0));
  }
}

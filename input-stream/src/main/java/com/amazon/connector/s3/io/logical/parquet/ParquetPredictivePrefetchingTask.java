package com.amazon.connector.s3.io.logical.parquet;

import com.amazon.connector.s3.io.logical.LogicalIOConfiguration;
import com.amazon.connector.s3.io.physical.PhysicalIO;
import com.amazon.connector.s3.io.physical.plan.IOPlan;
import com.amazon.connector.s3.io.physical.plan.Range;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task for predictively prefetching columns of a parquet file. Works by keeping track of recent
 * columns read from previously opened parquet files, and if the currently open parquet file has a
 * recent column, prefetch it.
 */
public class ParquetPredictivePrefetchingTask {

  private final PhysicalIO physicalIO;
  private final LogicalIOConfiguration logicalIOConfiguration;

  private static final Logger LOG = LoggerFactory.getLogger(ParquetPredictivePrefetchingTask.class);

  /**
   * Creates a new instance of {@link ParquetPredictivePrefetchingTask}
   *
   * @param logicalIOConfiguration logical io configuration
   * @param physicalIO PhysicalIO instance
   */
  public ParquetPredictivePrefetchingTask(
      @NonNull LogicalIOConfiguration logicalIOConfiguration, @NonNull PhysicalIO physicalIO) {
    this.physicalIO = physicalIO;
    this.logicalIOConfiguration = logicalIOConfiguration;
  }

  /**
   * Checks if the current position corresponds to a column, and if yes, adds it to the recent
   * columns list.
   *
   * @param position current read position
   * @return name of column added as recent column
   */
  public Optional<String> addToRecentColumnList(long position) {
    if (logicalIOConfiguration.isPredictivePrefetchingEnabled()
        && physicalIO.columnMappers() != null) {
      ColumnMappers columnMappers = physicalIO.columnMappers();
      if (columnMappers.getOffsetIndexToColumnMap().containsKey(Long.toString(position))) {
        String recentColumnName =
            columnMappers.getOffsetIndexToColumnMap().get(Long.toString(position)).getColumnName();
        physicalIO.addRecentColumn(recentColumnName);
        return Optional.of(recentColumnName);
      }
    }

    return Optional.empty();
  }

  /**
   * If any recent columns exist in the current parquet file, prefetch them.
   *
   * @param columnMappersOptional Optional of parquet file column mappings
   * @return ranges prefetched
   */
  public Optional<List<Range>> prefetchRecentColumns(
      Optional<ColumnMappers> columnMappersOptional) {
    // TODO: currently only handles single row groups.
    if (logicalIOConfiguration.isPredictivePrefetchingEnabled()
        && columnMappersOptional.isPresent()) {
      List<Range> prefetchRanges = new ArrayList<>();
      ColumnMappers columnMappers = columnMappersOptional.get();
      for (String recentColumn : physicalIO.getRecentColumns()) {
        if (columnMappers.getColumnNameToColumnMap().containsKey(recentColumn)) {
          ColumnMetadata columnMetadata =
              columnMappers.getColumnNameToColumnMap().get(recentColumn);
          prefetchRanges.add(
              new Range(
                  columnMetadata.getStartPos(),
                  columnMetadata.getStartPos() + columnMetadata.getCompressedSize()));
        }
      }

      IOPlan ioPlan = IOPlan.builder().prefetchRanges(prefetchRanges).build();
      try {
        physicalIO.execute(ioPlan);
        return Optional.of(prefetchRanges);
      } catch (IOException e) {
        LOG.debug("Error in executing predictive prefetch plan", e);
      }
    }

    return Optional.empty();
  }
}

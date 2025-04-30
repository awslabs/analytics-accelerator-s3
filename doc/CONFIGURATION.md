# S3 Analytics Accelerator Configuration

The Analytics Accelerator Library for Amazon S3 provides configuration options to optimize S3 data access across different frameworks. All configuration properties follow a prefix pattern based on the connector being used.

## Configuration Structure

The configuration options are organized into the following main sections:

- [Logical IO Configuration](#logical-io-configuration) (`logicalio`)
- [Physical IO Configuration](#physical-io-configuration) (`physicalio`)
- [Telemetry Configuration](#telemetry-configuration) (`telemetry`)
- [Object Client Configuration](#object-client-configuration)

### Configuration Prefix

All properties require a connector-specific prefix (`<CONNECTOR_PREFIX>`). This prefix varies depending on the framework:
- For Apache Hadoop S3A:
  - Standalone Hadoop applications: `hadoop.fs.s3a.analytics.accelerator`
  - Within Spark applications: `spark.hadoop.fs.s3a.analytics.accelerator`
- For Apache Iceberg S3FileIO: `spark.sql.catalog.spark_catalog.s3.analytics-accelerator`

### Usage Example

Here's an example using Apache Iceberg S3FileIO prefix. For other frameworks, simply replace the prefix while keeping the rest of the property name the same:

```properties
# Enable analytics accelerator
spark.sql.catalog.spark_catalog.s3.analytics-accelerator.enabled=true

# example configurations
spark.sql.catalog.spark_catalog.s3.analytics-accelerator.logicalio.prefetch.footer.enabled=true
spark.sql.catalog.spark_catalog.s3.analytics-accelerator.logicalio.prefetching.mode=ROW_GROUP
spark.sql.catalog.spark_catalog.s3.analytics-accelerator.physicalio.blocksizebytes=8388608
spark.sql.catalog.spark_catalog.s3.analytics-accelerator.telemetry.logging.enabled=true
spark.sql.catalog.spark_catalog.s3.analytics-accelerator.telemetry.logging.level=INFO
spark.sql.catalog.spark_catalog.s3.analytics-accelerator.useragentprefix=my-application
```


## Logical IO Configuration
Options under `<CONNECTOR_PREFIX>.logicalio.`

| Option                                   | Default               | Description                                                                           |
|------------------------------------------|-----------------------|---------------------------------------------------------------------------------------|
| `prefetch.footer.enabled`                | `true`                | Controls whether footer prefetching is enabled                                        |
| `prefetch.page.index.enabled`            | `true`                | Controls whether page index prefetching is enabled                                    |
| `use.format.specific.io`                 | `true`                | Controls whether to use format-specific LogicalIO implementations                     |
| `prefetch.file.metadata.size`            | `32KB`                | Size of metadata to prefetch for regular files                                        |
| `prefetch.large.file.metadata.size`      | `1MB`                 | Size of metadata to prefetch for large files                                          |
| `prefetch.file.page.index.size`          | `1MB`                 | Size of page index to prefetch for regular files                                      |
| `prefetch.large.file.page.index.size`    | `8MB`                 | Size of page index to prefetch for large files                                        |
| `large.file.size`                        | `1GB`                 | Threshold to consider a file as large                                                 |
| `small.objects.prefetching.enabled`      | `true`                | Controls prefetching for small objects                                                |
| `small.object.size.threshold`            | `3MB`                 | Size threshold for small object prefetching                                           |
| `parquet.metadata.store.size`            | `45`                  | Size of the parquet metadata store                                                    |
| `max.column.access.store.size`           | `15`                  | Maximum size of column access store                                                   |
| `partition.size`                         | `128MB`               | Size of partition for prefetching data in sequential access formats (csv, json, txt)  |
| `parquet.format.selector.regex`          | `^.*.(parquet\|par)$` | Regex pattern to identify parquet files                                               |
| `csv.format.selector.regex`              | `^.*\\.(csv\|CSV)$`   | Regex pattern to identify csv files                                                   |
| `json.format.selector.regex`             | `^.*\\.(json\|JSON)$` | Regex pattern to identify json files                                                  |
| `txt.format.selector.regex`              | `^.*\\.(txt\|TXT)$`   | Regex pattern to identify txt files                                                   |
| `prefetching.mode`                       | `ROW_GROUP`           | Prefetching mode (valid values: `OFF`, `ALL`, `ROW_GROUP`, `COLUMN_BOUND`)            |

## Physical IO Configuration
Options under `<CONNECTOR_PREFIX>.physicalio.`

| Option                     | Default | Description                                   |
|----------------------------|---------|-----------------------------------------------|
| `metadatastore.capacity`   | `50`    | Capacity of the metadata store                |
| `blocksizebytes`           | `8MB`   | Size of blocks for data transfer              |
| `readaheadbytes`           | `64KB`  | Number of bytes to read ahead                 |
| `maxrangesizebytes`        | `8MB`   | Maximum size of range requests                |
| `partsizebytes`            | `8MB`   | Size of individual parts for transfer         |
| `sequentialprefetch.base`  | `2.0`   | Base factor for sequential prefetch sizing    |
| `sequentialprefetch.speed` | `1.0`   | Speed factor for sequential prefetch growth   |

## Telemetry Configuration
Options under `<CONNECTOR_PREFIX>.telemetry.`

| Option                                | Default                                             | Description                                                              |
|---------------------------------------|-----------------------------------------------------|--------------------------------------------------------------------------|
| `level`                               | `STANDARD`                                          | Telemetry detail level (valid values: `CRITICAL`, `STANDARD`, `VERBOSE`) |
| `std.out.enabled`                     | `false`                                             | Enable stdout telemetry output                                           |
| `logging.enabled`                     | `true`                                              | Enable logging telemetry output                                          |
| `aggregations.enabled`                | `false`                                             | Enable telemetry aggregations                                            |
| `aggregations.flush.interval.seconds` | `-1`                                                | Interval to flush aggregated telemetry                                   |
| `logging.level`                       | `DEBUG`                                             | Log level for telemetry                                                  |
| `logging.name`                        | `software.amazon.s3.analyticsaccelerator.telemetry` | Logger name for telemetry                                                |
| `format`                              | `default`                                           | Telemetry output format (valid values: `json`, `default`)                |

## Object Client Configuration
Options under `<CONNECTOR_PREFIX>.`

| Option            | Default | Description                                                    |
|-------------------|---------|----------------------------------------------------------------|
| `useragentprefix` | `null`  | Custom prefix to add to the `User-Agent` string in S3 requests |

- The Object Client Configuration also includes its own telemetry settings under `<CONNECTOR_PREFIX>.telemetry.` which follow the same structure as the main [Telemetry Configuration](#telemetry-configuration) section.

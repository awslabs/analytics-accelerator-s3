# Analytics Accelerator Library for Amazon S3

The Analytics Accelerator Library for Amazon S3 helps you accelerate access to Amazon S3 data from your applications. This open-source solution reduces processing times and compute costs for your data analytics workloads.

With this library, you can: 
* Lower processing times and compute costs for data analytics workloads.
* Implement S3 best practices for performance. 
* Utilize optimizations specific to [Apache Parquet](https://parquet.apache.org/) files, such as pre-fetching metadata located in the footer of the object and predictive column pre-fetching.
* Improve the price performance for your data analytics applications, such as workloads based on [Apache Spark](https://spark.apache.org/). 

## Current Status

The Analytics Accelerator Library for Amazon S3 has been tested and integrated with the [Apache Hadoop S3A](https://github.com/apache/hadoop) client, and is currently in the process of being integrated into the [Iceberg S3FileIO](https://github.com/apache/iceberg) client.
It is also tested for datasets stored in [S3 Table Buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets.html).

We're constantly working on improving Analytics Accelerator Library for Amazon S3 and are interested in hearing your feedback on features, performance, and compatibility. Please send feedback by [opening a GitHub issue](https://github.com/awslabs/analytics-accelerator-s3/issues/new/choose).

## Getting Started

You can use Analytics Accelerator Library for Amazon S3 either as a standalone library or with Hadoop library and Spark engine.
For Spark engine, it also supports [Iceberg](https://github.com/apache/iceberg)'s open table format as well as [S3 Table Buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets.html).
Integrations with Hadoop and Iceberg are turned off by default. We describe how to enable Analytics Accelerator Library for Amazon S3 below.


### Standalone Usage

To get started, import the library dependency from Maven into your project:

```
    <dependency>
      <groupId>software.amazon.s3.analyticsaccelerator</groupId>
      <artifactId>analyticsaccelerator-s3</artifactId>
      <version>1.0.0</version>
      <scope>compile</scope>
    </dependency>
```

Then, initialize the library `S3SeekableInputStreamFactory`

```
S3AsyncClient crtClient = S3CrtAsyncClient.builder().maxConcurrency(600).build();
S3SeekableInputStreamFactory s3SeekableInputStreamFactory = new S3SeekableInputStreamFactory(
                new S3SdkObjectClient(this.crtClient), S3SeekableInputStreamConfiguration.DEFAULT);
```


**Note:** The `S3SeekableInputStreamFactory` can be initialized with either the [S3AsyncClient](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3AsyncClient.html) or the [S3 CRT client](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/crt-based-s3-client.html). 
We recommend that you use the S3 CRT client due to its enhanced connection pool management and [higher throughput on downloads](https://aws.amazon.com/blogs/developer/introducing-crt-based-s3-client-and-the-s3-transfer-manager-in-the-aws-sdk-for-java-2-x/). 
For either client, we recommend you initialize with a higher concurrency value to fully benefit from the library's optimizations. 
This is because the library makes multiple parallel requests to S3 to prefetch data asynchronously. For the Java S3AsyncClient, you can increase the maximum connections by doing the following:

```
NettyNioAsyncHttpClient.Builder httpClientBuilder =
        NettyNioAsyncHttpClient.builder()
        .maxConcurrency(600);

S3AsyncClient s3AsyncClient = S3AsyncClient.builder().httpClientBuilder(httpClientBuilder).build(); 
```

To open a stream: 

```
S3SeekableInputStream s3SeekableInputStream = s3SeekableInputStreamFactory.createStream(S3URI.of(bucket, key));
```

For more details on the usage of this stream, refer to the [SeekableInputStream](https://github.com/awslabs/analytics-accelerator-s3/blob/main/input-stream/src/main/java/software/amazon/s3/analyticsaccelerator/SeekableInputStream.java) interface.

When the `S3SeekableInputStreamFactory` is no longer required to create new streams, close it to free resources (eg: caches for prefetched data) held by the factory. 

```
s3SeekableInputStreamFactory.close();
```

### Using with Hadoop

If you are using Analytics Accelerator Library for Amazon S3 with Hadoop, you need to set the stream type to `analytics` in the Hadoop configuration. An example configuration is as follows:

```
<property>
  <name>fs.s3a.input.stream.type</name>
  <value>analytics</value>
</property>
```

For more information, see the Hadoop documentation on the [Analytics stream type](https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/site/markdown/tools/hadoop-aws/reading.md#stream-type-analytics).

### Using with Spark (without Iceberg)

Using the Analytics Accelerator Library for Amazon S3 with Spark is similar to the Hadoop usage. The only difference is to prepend the property names with `spark.hadoop`.
For example, to enable Analytics Accelerator Library for Amazon S3 on Spark engine, you need to set following property in Spark configuration.

```
<property>
  <name>spark.hadoop.fs.s3a.input.stream.type</name>
  <value>analytics</value>
</property>
```

### Using with Spark (with Iceberg)

When your data in S3 is organised in Iceberg open table format, data will be retrieved using [Iceberg S3FileIO](https://github.com/apache/iceberg) client instead of [Hadoop S3A](https://github.com/apache/hadoop) client.
Analytics Accelerator Library for Amazon S3 is currently being integrated into [Iceberg S3FileIO](https://github.com/apache/iceberg) client. To enable it in the Spark engine, set the following Spark property.

```
<property>
  <name>spark.sql.catalog.<CATALOG_NAME>.s3.analytics-accelerator.enabled</name>
  <value>true</value>
</property>
```

For [S3 General Purpose Buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingBucket.html) and [S3 Directory Buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-overview.html) set the `<CATALOG_NAME>`to `spark_catalog`, the default catalog. 

[S3 Table Buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets.html) require you to set a custom catalog name, as outlined [here](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source-spark.html).
Once you set the catalog, you can replace the `<CATALOG_NAME>` parameter with your chosen name.


To learn more about how to set rest of the configurations, read our [configuration](doc/CONFIGURATION.md) documents.


## Summary of Optimizations

Analytics Accelerator Library for Amazon S3 accelerates read performance of objects stored in Amazon S3 by integrating AWS Common Run Time (CRT) libraries and implementing optimizations specific to Apache Parquet files. The AWS CRT is a software library built for interacting with AWS services, that implements best practice performance design patterns, including timeouts, retries, and automatic request parallelization for high throughput. 
You can use `S3SeekableInputStreamFactory` to initialize streams for all file types to benefit from read optimizations on top of benefits coming from CRT. 

These optimizations are:

* Sequential prefetching - The library detects sequential read patterns to prefetch data and reduce latency, and reads the full object when the object is small to minimize the number of read operations.
* Small object prefetching - The library will prefetch the object if the object size is less than 3MB.

 When the object key ends with the file extension `.parquet` or `.par`, we use the following Apache Parquet specific optimizations:

* Parquet footer caching - The library reads the tail of the object with configurable size (1MB by default) as soon as a stream to a Parquet object is opened and caches it in memory. This is done to prevent multiple small GET requests that occur at the tail
  of the file for the Parquet metadata, `pageIndex`, and bloom filter structures. 
* Predictive column prefetching - The library tracks recent columns being read using parquet metadata. When
  subsequent Parquet files which have these columns are opened, the library will prefetch these columns. For example, if columns `x` and `y` are read from `A.parquet` , and then `B.parquet` is opened, and it also contains columns named `x` and `y`, the library will prefetch them asynchronously.

## Memory Used by Library
Analytics Accelerator Library for Amazon S3 implements a best-effort memory limiting mechanism. The library fetches data from S3 in blocks of bytes and keeps them in memory. Memory management is achieved through a dual strategy combining Time-to-Live (TTL) and maximum memory threshold.
When time to live or memory usage exceeds the configured threshold, blocks to be removed are identified using [Time_based_eviction](https://github.com/ben-manes/caffeine/wiki/Eviction#time-based) and [Window TinyLfu algorithm](https://github.com/ben-manes/caffeine/wiki/Efficiency) respectively, implemented by [Caffeine library](https://github.com/ben-manes/caffeine/blob/master/README.md). Removal is done using an async process that runs at configured intervals, meaning memory usage might temporarily exceed the threshold. This overflow period can be minimized by increasing the cleanup frequency, though at the cost of higher CPU utilization.
You can change TTL, memory usage threshold and cleanup frequency as follows:
Note: We allow only positive values for the below configs.
* Memory limit can be set using the key `max.memory.limit` by default which is `2GB`. Take into consideration workload and system resources when configuring this value. For eg: For parquet workload consider factors like row group size and number of vCPUs on executors.
* Cache data timeout can be set using the key `cache.timeout` by default which is `1s`.
* Cleanup frequency can be set using the key `memory.cleanup.frequency` by default which is `5s`.
To learn more about how to set the configurations, read our [configuration](doc/CONFIGURATION.md) documents.

## Benchmark Results 

### Benchmarking Results -- November 25, 2024

The current benchmarking results are provided for reference only. It is important to note that the performance of these queries can be affected by a variety of factors, including compute and storage variability, cluster configuration, and compute choice. All of the results presented have a margin of error of up to 3%.

To establish the performance impact of changes, we rely on a benchmark derived from an industry standard TPC-DS benchmark at a 3 TB scale. It is important to note that our TPC-DS derived benchmark results are not directly comparable with official TPC-DS benchmark results. We also found that the sizing of Apache Parquet files and partitioning of the dataset have a substantive impact on the workload performance. As a result, we have created several versions of the test dataset, with a focus on different object sizes, ranging from singular MiBs to tens of GiBs, as well as various partitioning approaches

On S3A, we have observed a total suite execution acceleration between 10% and 27%, with some queries showing a speed-up of up to 40%. 

**Known issue:** We are currently observing a regression of up to 8% on queries similar to the Q44 in [issue 173](https://github.com/awslabs/analytics-accelerator-s3/issues/173). We have determined the root cause of this issue is a data over-read due to overly eager columnar prefetching when all of the following is true: 
1. The query is filtering on dictionary encoded columns.
1. The query is selective, and most objects do not contain the required data.
1. The query operates on a multi-GB dataset.
   
We are actively working on this issue. You can track the progress in the [issue 173](https://github.com/awslabs/analytics-accelerator-s3/issues/173) page. 

The remaining TPC-DS queries show no regressions within the specified margin of error.

## Contributions

We welcome contributions to Analytics Accelerator Library for Amazon S3! See the [contributing guidelines](doc/CONTRIBUTING.md) for more information on how to report bugs, build from source code, or submit pull requests.

## Security

If you discover a potential security issue in this project we ask that you notify Amazon Web Services (AWS) Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Do **not** create a public GitHub issue.

## License

Analytics Accelerator Library for Amazon S3 is licensed under the [Apache-2.0 license](LICENSE). 
The pull request template will ask you to confirm the licensing of your contribution and to agree to the [Developer Certificate of Origin (DCO)](https://developercertificate.org/).

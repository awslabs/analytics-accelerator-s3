# Analytics Accelerator Library for Amazon S3

Analytics Accelerator Library for Amazon S3 is an open source library that accelerates data access from client applications to Amazon S3. 

With this library, you can: 
* Lower processing times and compute costs for data analytics workloads.
* Implement S3 best practices for performance. 
* Utilize optimizations specific to [Apache Parquet](https://parquet.apache.org/) files, such as pre-fetching metadata located in the footer of the object and predictive column pre-fetching.
* Improve the price performance for your data analytics applications, such as workloads based on [Apache Spark](https://spark.apache.org/). 

## Current status

Analytics Accelerator Library for Amazon S3 is **currently an alpha release and should not be used in production**. We're especially interested in early feedback on features, performance, and compatibility. Please send feedback by [opening a GitHub issue](https://github.com/awslabs/analytics-accelerator-s3/issues/new/choose).

## Getting Started

Alpha version of the library provides an interface for a seekable input stream. The library is currently being integrated and tested with the [Apache Hadoop S3A](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#Introducing_the_Hadoop_S3A_client.) client.

To get started, import the library dependency from Maven into your project:

```
    <dependency>
      <groupId>software.amazon.s3.analyticsaccelerator</groupId>
      <artifactId>analyticsaccelerator-s3</artifactId>
      <version>0.0.2</version>
      <scope>compile</scope>
    </dependency>
```

Then, initialize the library `S3SeekableInputStreamFactory`

```
S3AsyncClient crtClient = S3CrtAsyncClient.builder().maxConcurrency(600).build();
S3SeekableInputStreamFactory s3SeekableInputStreamFactory = new S3SeekableInputStreamFactory(
                new S3SdkObjectClient(this.crtClient), S3SeekableInputStreamConfiguration.DEFAULT);
```


**Note:** The `S3SeekableInputStreamFactory` can be initialized with either the [S3AsyncClient](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/s3/S3AsyncClient.html) or the [S3 CRT client](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/crt-based-s3-client.html). We recommend that you use the S3 CRT client due to its enhanced connection pool management and [higher throughput on downloads](https://aws.amazon.com/blogs/developer/introducing-crt-based-s3-client-and-the-s3-transfer-manager-in-the-aws-sdk-for-java-2-x/). For either client, we recommend you initialize with a higher concurrency value to fully benefit from the library's optimizations. This is because the library makes multiple parallel requests to S3 to prefetch data asynchronously. For the Java S3AsyncClient, you can increase the maximum connections by doing the following:

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

For more details on the usage of this stream, please refer to the [SeekableInputStream](https://github.com/awslabs/analytics-accelerator-s3/blob/main/input-stream/src/main/java/software/amazon/s3/analyticsaccelerator/SeekableInputStream.java) interface.

When the `S3SeekableInputStreamFactory` is no longer required to create new streams, close it to free resources (eg: caches for prefetched data) held by the factory. 

```
s3SeekableInputStreamFactory.close();
```

## Summary of Optimizations

Analytics Accelerator Library for Amazon S3 accelerates read performance of objects stored in Amazon S3 by integrating AWS Common Run Time (CRT) libraries and implementing optimizations specific to Apache Parquet files. The AWS CRT is a software library built for interacting with AWS services, that implements best practice performance design patterns, including timeouts, retries, and automatic request parallelization for high throughput. `S3SeekableInputStreamFactory` can be used to initialize streams for all file types to benefit from read optimizations on top of benefits coming from CRT. 

These optimizations are:

* Sequential prefetching - The library detects sequential read patterns to prefetch data and reduce latency, and reads the full object when the object is small to minimize the number of read operations.
* Small object prefetching - The library will prefetch the object if the object size is less than 3MB.

 When the object key ends with the file extension `.parquet` or `.par`, we use the following Apache Parquet specific optimizations:

* Parquet footer caching - The library reads the tail of the object with configurable size (1MB by default) as soon as a stream to a Parquet object is opened and caches it in memory. This is done to prevent multiple small GET requests that occur at the tail
  of the file for the Parquet metadata, `pageIndex`, and bloom filter structures. 
* Predictive column prefetching - The library tracks recent columns being read using parquet metadata. When
  subsequent Parquet files which have these columns are opened, the library will prefetch these columns. For example, if columns `x` and `y` are read from `A.parquet` , and then `B.parquet` is opened, and it also contains columns named `x` and `y`, the library will prefetch them asynchronously.

## Benchmark Results 

### Benchmarking Results -- November 25, 2024

The current benchmarking results are provided for reference only. It is important to note that the performance of these queries can be affected by a variety of factors, including compute and storage variability, cluster configuration, and compute choice. All of the results presented have a margin of error of up to 3%.

To establish the performance impact of changes, we rely on a benchmark derived from an industry standard TPC-DS benchmark at a 3 TB scale. It is important to note that our TPC-DS derived benchmark results are not directly comparable with official TPC-DS benchmark results. We also found that the sizing of Apache Parquet files and partitioning of the dataset have a substantive impact on the workload performance. As a result, we have created several versions of the test dataset, with a focus on different object sizes, ranging from singular MiBs to tens of GiBs, as well as various partitioning approaches

On S3A, we have observed a total suite execution acceleration between 10% and 27%, with some queries showing a speed-up of up to 40%. 

## Contributions

We welcome contributions to Analytics Accelerator Library for Amazon S3! Please see the [contributing guidelines](doc/CONTRIBUTING.md) for more information on how to report bugs, build from source code, or submit pull requests.

## Security

If you discover a potential security issue in this project we ask that you notify Amazon Web Services (AWS) Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

## License

Analytics Accelerator Library for Amazon S3 is licensed under the [Apache-2.0 license](LICENSE). 
The pull request template will ask you to confirm the licensing of your contribution and to agree to the [Developer Certificate of Origin (DCO)](https://developercertificate.org/).

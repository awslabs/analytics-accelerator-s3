# Microbenchmarking

Microbenchmarks can be used to assess the performance of the library. 
These focus on replicating specific use cases, and comparing performance with 
reading from the Java SDK clients directly. 

## Supported Benchmarks

This section details the current micro benchmarks that can be used. Currently, we have two benchmarks:
* ConcurrentStreamPerformanceBenchmark: This benchmark replicates concurrency and read patterns when executing a Spark
query. It will run concurrent streams, each of which will make 5 ranged GETS, to represent Parquet footer and column
reads. This can be run on any dataset (it doesn't have to be Parquet), the microbenchmark will just do parquet like 
reads.
* SequentialStreamPerformanceBenchmark: This benchmark reads through a file sequentially. It is useful to understand 
the benefits of the library's sequential prefetching, and the performance is compared to just reading through a stream
with the SDK directly. 

### ConcurrentStreamPerformanceBenchmark 

This benchmark replicates what would happen on a single Spark node:

Given a table of data that contains multiple objects, it will process each file in parallel.

* It uses `Runtime.getRuntime().availableProcessors()` to find the current number of cores on the compute node.
* It opens concurrent streams to objects in the table. Number of concurrent streams is equal to the number of available
processors. Each stream represents a spark task.
* For each stream, we do reads which replicate how a Parquet file would be processed:
  * Do the Parquet footer dance (Read the last 8 bytes, and then the last 4KB) for the footer.
  * Then at different points of the file, read ranges of 5-10MB. Each of these ranges represent a parquet column read.

We do the above with AAL, and with the Java clients. 

To run this benchmark

```
// Build the JAR
./gradlew jmhJar

// Set the environment

// This should be a bucket which contains your dataset, ideally with a few hundred files. 
// Can be of any file format, benchmark will just do a parquet read pattern on it.
export S3_DATASET_BUCKET=<BUCKET>

// Prefix data is under in the given bucket
export  S3_TEST_PREFIX=<DATA_PREFIX>

// Run the benchmark
java -Xmx8g -Xms4g -jar input-stream/build/libs/input-stream-jmh.jar "ConcurrentStreamPerformanceBenchmark" 
```

### SequentialStreamPerformanceBenchmark

This benchmark will read through a given file in a sequence of 8KB reads. This represents what would happen when
reading sequential file formats, as these readers typically read in 8KB buffers. 

To run this benchmark

```
// Build the JAR
./gradlew jmhJar

// Set the environment

// Bucket which has the file you want to read sequentially
export S3_DATASET_BUCKET=<BUCKET>

// Key for the object to be read sequentially. 
export S3_OBJECT_KEY=<OBJECT_KEY>

// Run the benchmark
java -Xmx8g -Xms4g  -jar input-stream/build/libs/input-stream-jmh.jar "SequentialStreamPerformanceBenchmark"
```

## Profiling

A number of different profilers with microbenchmarks to analyse performance. To use the async profiler, 
do the following:

#### Install the Async profiler

```
// Download latest release
wget https://github.com/async-profiler/async-profiler/releases/download/v3.0/async-profiler-3.0-linux-x64.tar.gz

// Extract
tar -xzf async-profiler-3.0-linux-x64.tar.gz

// Add to library path
export LD_LIBRARY_PATH=$PWD/async-profiler-3.0-linux-x64/lib:$LD_LIBRARY_PATH
```

#### Run micro benchmarks with profiler attached

Use the `-prof async:output=flamegraph`. This will output html files with flamegraphs for each test case in 
the microbenchmark.

```
java -Xmx8g -Xms4g -jar input-stream/build/libs/input-stream-jmh.jar "ConcurrentStreamPerformanceBenchmark" -prof async:output=flamegraph
```
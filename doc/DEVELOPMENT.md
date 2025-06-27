# DEVELOPMENT

The project uses Gradle 8.7 and targets Java 8, which is currently the most common Java version in Data Lake stacks.

**Please be careful to not accidentally take a dependency on Java 11 and above - this is a one-way door**

## Building From Source
* To invoke a build and run tests: `./gradlew build`
* To run reference tests: `./gradlew referenceTest`
* To apply formatting: `./gradlew spotlessApply`
* To list all tasks: `./gradlew tasks`
* To publish JARs to Maven local: `./gradlew publishToMavenLocal`
* To build JMH benchmarks JAR: `./gradlew jmhJar`

## Microbenchmarks and Integration Tests
We have a basic set of micro-benchmarks that test full sequential read, forward seeks, backward seeks, and a Parquet-like ("jumping around") read pattern.
We also have a number of integration tests that test correctness, exception handling, concurrency correctness and gray failures.
The configuration and data generation steps are pre-requisite for running the integration tests and microbenchmarks.


### Configuration
Configure the following environment variables before generating data and running benchmarks:
* `S3_TEST_BUCKET` - the bucket benchmarks and the data generation runs against.
* `S3_TEST_REGION` - the region the bucket belongs to.
* `S3_TEST_PREFIX` - the prefix within the bucket that benchmarks and the data generation runs against
* `CUSTOMER_KEY` - the Base64-encoded encryption key that enables Server-Side Encryption with Customer-Provided Keys (SSE-C) for S3 object uploads.

### Data Generation
After your environment is configured, you can generate data to run benchmarks against.
* If you haven't already done so, create a new S3 Bucket and add the bucket name(`S3_TEST_BUCKET`) and prefix(`S3_TEST_PREFIX`) as env vars
* Build the `jmhJar` : `./gradlew jmhJar`
* Run the generator: `java -cp input-stream/build/libs/input-stream-jmh.jar software.amazon.s3.analyticsaccelerator.benchmarks.data.generation.BenchmarkDataGeneratorDriver`

This will generate all the necessary data and upload it to the bucket and prefix identified by the `S3_TEST_BUCKET` and `S3_TEST_PREFIX` respectively.

### Running Integration Tests
Before running integration tests, you'll need test data created by the 'Data Generation' process. 
This is a one-time setup - you won't need to regenerate the data for subsequent test runs. 
Note that these tests are specifically designed for this generated data and won't work with other data in your S3 bucket.


Once you've created the benchmark data and configured your environment variables, you can proceed with running the integration tests.
* `./gradlew integrationTest`

### Running the Benchmarks
There are two ways:
1. Just run `./gradlew jmh --rerun`. (The reason for re-run is a Gradle-quirk. You may want to re-run benchmarks even when
   you did not actually change the source of your project: `--rerun` turns off the Gradle optimisation that falls through
   build steps when nothing changed.)
2. Run `java -jar input-stream/build/libs/input-stream-jmh.jar` (but don't forget to build the JMH JAR first, which you can do with the `jmhJar` command listed above). 

## Developing integrations

When you are building this library into connectors, your IDE will need to be aware of the JARs. 
Consuming these via Maven/Gradle is natural, and you can use `./gradlew publishToMavenLocal` to have the built JARs installed to your Maven local repository (this is `~/.m2` most of the time).
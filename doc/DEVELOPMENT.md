# DEVELOPMENT

The project is configured to be built via Gradle (Gradle 8.7). It also targets Java 8, as this is the most commonly used Java version in Data Lake stacks right now.

**Please be careful to not accidentally take a dependency on Java 11 and above - this is a one-way door**

## Building From Source
* To invoke a build and run tests: `./gradlew build`
* To run reference tests: `./gradlew referenceTest`
* To apply formatting: `./gradlew spotlessApply`
* To list all tasks: `./gradlew tasks`
* To publish JARs to Maven local: `./gradlew publishToMavenLocal`
* To build JMH benchmarks JAR: `./gradlew jmhJar`

## Microbenchmarks
We have a basic set of micro-benchmarks which test full sequential read, forward seeks, backward seeks and a
Parquet-like ("jumping around") read pattern.
### Configuration
To generate data and run benchmarks, you first need to configure your environment variables. They are as follows:
* `S3_TEST_BUCKET` - the bucket benchmarks and the data generation runs against.
* `S3_TEST_REGION` - the region the bucket belongs to.
* `S3_TEST_PREFIX` - the prefix within the bucket that benchmarks and the data generation runs against

### Data Generation
After your environment is configured, you can generate data to run benchmarks against.
* If you haven't already done so create a new S3 Bucket and add the bucket name(`S3_TEST_BUCKET`) and prefix(`S3_TEST_PREFIX`) as env vars
* Build the `jmhJar` : `./gradlew jmhJar`
* Run the generator: `java -cp input-stream/build/libs/input-stream-jmh.jar software.amazon.s3.analyticsaccelerator.benchmarks.data.generation.BenchmarkDataGeneratorDriver`

This will generate all the necessary data and upload it to the bucket and prefix identified by the `S3_TEST_BUCKET` and `S3_TEST_PREFIX` respectively.

### Running the Benchmarks
There are two ways:
1. Just run `./gradlew jmh --rerun`. (The reason for re-run is a Gradle-quirk. You may want to re-run benchmarks even when
   you did not actually change the source of your project: `--rerun` turns off the Gradle optimisation that falls through
   build steps when nothing changed.)
2. Run `java -jar input-stream/build/libs/input-stream-jmh.jar` (but don't forget to build the JMH JAR first; this you
   can do with the `jmhJar` command listed above). 

## Developing integrations

When you are building this library into connectors, your IDE will need to be aware of the JARs. 
Consuming these via Maven/Gradle is natural, and you can use `./gradlew publishToMavenLocal` to have the built JARs installed to your Maven local repository (this is `~/.m2` most of the time).
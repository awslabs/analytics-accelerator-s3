## v1.3.1 (November 10, 2025)
* Coalesce ranges for vectored reads https://github.com/awslabs/analytics-accelerator-s3/pull/356
* Adds support for IoStats for S3A https://github.com/awslabs/analytics-accelerator-s3/pull/358
* Adds exception to cause during exception translation https://github.com/awslabs/analytics-accelerator-s3/pull/361

## v1.3.0 (August 20, 2025)
* Introduce max read bytes config for sequential read https://github.com/awslabs/analytics-accelerator-s3/pull/348
* Add TTL to metadata store cache https://github.com/awslabs/analytics-accelerator-s3/pull/338
* Update microbenchmarks to better reflect a Spark workload https://github.com/awslabs/analytics-accelerator-s3/pull/334
* Add support for Java sync client https://github.com/awslabs/analytics-accelerator-s3/pull/341
* Calls release() method on any failure https://github.com/awslabs/analytics-accelerator-s3/pull/337
* New PhysicalIO implementation https://github.com/awslabs/analytics-accelerator-s3/pull/325
* Upgrade SDK version and fix user-agent changes to avoid prepending empty string https://github.com/awslabs/analytics-accelerator-s3/pull/319
* Upgrade to junit 5.13.4 https://github.com/awslabs/analytics-accelerator-s3/pull/314
* Upgrade to freefair lombok version 8.14 https://github.com/awslabs/analytics-accelerator-s3/pull/313
* Append AAL user agent to customer user agent https://github.com/awslabs/analytics-accelerator-s3/pull/311
* Add Retries and Retry Policy https://github.com/awslabs/analytics-accelerator-s3/pull/307

## v1.2.1 (July 07, 2025)

* fix: shading dependency on google error prone https://github.com/awslabs/analytics-accelerator-s3/pull/305
* IoStats-CallBack-AAL https://github.com/awslabs/analytics-accelerator-s3/pull/298
* Improves integration test cases https://github.com/awslabs/analytics-accelerator-s3/pull/297
* Generate encrypted objects for benchmark https://github.com/awslabs/analytics-accelerator-s3/pull/296

## v1.2.0 (Jun 25, 2025)

* Replace DAT with AAL https://github.com/awslabs/analytics-accelerator-s3/pull/300
* Add support for readFully at the S3SeekableInputStream level https://github.com/awslabs/analytics-accelerator-s3/pull/293
* Adds test cases for readVectored() https://github.com/awslabs/analytics-accelerator-s3/pull/284
* Update-integration-tests-worflow https://github.com/awslabs/analytics-accelerator-s3/pull/291
* SSE_C changes https://github.com/awslabs/analytics-accelerator-s3/pull/281
* Adds in support for S3A's auditing
* Pass down OpenStreamInformation to objectClient https://github.com/awslabs/analytics-accelerator-s3/pull/283
* Migration to Central Portal for Maven https://github.com/awslabs/analytics-accelerator-s3/pull/282
* Implements readVectored() in AAL https://github.com/awslabs/analytics-accelerator-s3/pull/270
* Fix bug in small object prefetching https://github.com/awslabs/analytics-accelerator-s3/pull/279
* Modify gradle setup step to reduce time https://github.com/awslabs/analytics-accelerator-s3/pull/278
* Run integration tests on self hosted runner https://github.com/awslabs/analytics-accelerator-s3/pull/277
* Introduces common executor pool https://github.com/awslabs/analytics-accelerator-s3/pull/275
* Fix fleaky unit tests on BlobStore https://github.com/awslabs/analytics-accelerator-s3/pull/273
* Move small object prefetching to physicalIO https://github.com/awslabs/analytics-accelerator-s3/pull/258
* Updated release.yml to fix signing issue https://github.com/awslabs/analytics-accelerator-s3/pull/269

## v1.1.0 (May 09, 2025)

* feat: Memory Manager https://github.com/awslabs/analytics-accelerator-s3/pull/251
* feat: Added new metrics like memory usage and cache hit/miss https://github.com/awslabs/analytics-accelerator-s3/pull/257
* feat: Read optimisations for sequential file formats https://github.com/awslabs/analytics-accelerator-s3/pull/238
* Improved integration test documentation https://github.com/awslabs/analytics-accelerator-s3/pull/260
* Added config to use format-specific LogicalIO implementations https://github.com/awslabs/analytics-accelerator-s3/pull/259
* Reduced waiting time and retry on GrayTest https://github.com/awslabs/analytics-accelerator-s3/pull/256
* fix: Failing ref tests https://github.com/awslabs/analytics-accelerator-s3/pull/255
* fix: Setting log path for telemetry https://github.com/awslabs/analytics-accelerator-s3/pull/252
* Added some debug logs https://github.com/awslabs/analytics-accelerator-s3/pull/250
* Reduced default block read timeout to 30 seconds https://github.com/awslabs/analytics-accelerator-s3/pull/249
* Enabled Iceberg unit-tests https://github.com/awslabs/analytics-accelerator-s3/pull/245

## v1.0.0 (March 04, 2025)

* Adds retrying of block reads https://github.com/awslabs/analytics-accelerator-s3/pull/229
* Logs everything at debug https://github.com/awslabs/analytics-accelerator-s3/pull/236
* Adds javadoc generation https://github.com/awslabs/analytics-accelerator-s3/pull/237

## v0.0.4 (February 21, 2025)

* Close input stream explicitly https://github.com/awslabs/analytics-accelerator-s3/pull/222
* Timeout retry stuck sdk client https://github.com/awslabs/analytics-accelerator-s3/pull/219
* Adds in constructor for open stream information https://github.com/awslabs/analytics-accelerator-s3/pull/223

## v0.0.3 (February 4, 2025)

* Fix Len = 0 and Insufficient Buffer behaviours for positioned reads https://github.com/awslabs/analytics-accelerator-s3/pull/203
* Support audit headers in request https://github.com/awslabs/analytics-accelerator-s3/pull/204
* fix: fixing jmh local build https://github.com/awslabs/analytics-accelerator-s3/pull/205
* Add ability to dump configs https://github.com/awslabs/analytics-accelerator-s3/pull/206
* Add JMH JAR generation to CICD https://github.com/awslabs/analytics-accelerator-s3/pull/207
* Migrate to new Iceberg staging branch https://github.com/awslabs/analytics-accelerator-s3/pull/208
* Improve the exception handling of the S3SdkObjectClient https://github.com/awslabs/analytics-accelerator-s3/pull/210
* Improve the unit and integration tests https://github.com/awslabs/analytics-accelerator-s3/pull/211
* feat: adding etag checking for stream reads https://github.com/awslabs/analytics-accelerator-s3/pull/209
* fix: updating integ test to check at correct point https://github.com/awslabs/analytics-accelerator-s3/pull/213
* add gray failure tests and FaultyS3Client https://github.com/awslabs/analytics-accelerator-s3/pull/214

## v0.0.2 (December 17, 2024)

* Fixed a typo in the README by @oleg-lvovitch-aws in https://github.com/awslabs/analytics-accelerator-s3/pull/180
* Remove unnecessary Maven publish step by @CsengerG in https://github.com/awslabs/analytics-accelerator-s3/pull/182
* Move both Iceberg and S3A CICD to snapshot builds by @CsengerG in https://github.com/awslabs/analytics-accelerator-s3/pull/186
* Split footer requests into two by @ahmarsuhail in https://github.com/awslabs/analytics-accelerator-s3/pull/188
* Prefetch dictionaries and column data separately by @ahmarsuhail in https://github.com/awslabs/analytics-accelerator-s3/pull/189
* Addresses review comments by @ahmarsuhail in https://github.com/awslabs/analytics-accelerator-s3/pull/190
* Add support to seek beyond end of stream @fuatbasik  (https://github.com/awslabs/analytics-accelerator-s3/pull/192)

## v0.0.1 (November 26, 2024)

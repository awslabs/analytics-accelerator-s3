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
package software.amazon.s3.analyticsaccelerator;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import software.amazon.s3.analyticsaccelerator.common.Metrics;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIO;
import software.amazon.s3.analyticsaccelerator.io.logical.LogicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetColumnPrefetchStore;
import software.amazon.s3.analyticsaccelerator.io.logical.impl.ParquetLogicalIOImpl;
import software.amazon.s3.analyticsaccelerator.io.physical.PhysicalIOConfiguration;
import software.amazon.s3.analyticsaccelerator.io.physical.data.BlobStore;
import software.amazon.s3.analyticsaccelerator.io.physical.data.MetadataStore;
import software.amazon.s3.analyticsaccelerator.io.physical.impl.PhysicalIOImpl;
import software.amazon.s3.analyticsaccelerator.util.FakeObjectClient;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class S3SeekableInputStreamTestBase {

  protected static final String TEST_DATA = "test-data12345678910";
  protected static final S3URI TEST_OBJECT = S3URI.of("bucket", "key");

  protected final PhysicalIOConfiguration physicalIOConfiguration = PhysicalIOConfiguration.DEFAULT;
  protected final FakeObjectClient fakeObjectClient = new FakeObjectClient(TEST_DATA);
  protected final MetadataStore metadataStore =
      new MetadataStore(
          fakeObjectClient,
          TestTelemetry.DEFAULT,
          PhysicalIOConfiguration.DEFAULT,
          mock(Metrics.class));
  protected final ExecutorService threadPool = Executors.newFixedThreadPool(30);
  protected final BlobStore blobStore =
      new BlobStore(
          fakeObjectClient,
          TestTelemetry.DEFAULT,
          physicalIOConfiguration,
          mock(Metrics.class),
          threadPool);
  protected final LogicalIOConfiguration logicalIOConfiguration = LogicalIOConfiguration.DEFAULT;

  protected final LogicalIO fakeLogicalIO;

  protected final ExecutorService executorService =
      Executors.newFixedThreadPool(physicalIOConfiguration.getThreadPoolSize());

  {
    try {
      fakeLogicalIO =
          new ParquetLogicalIOImpl(
              TEST_OBJECT,
              new PhysicalIOImpl(
                  TEST_OBJECT,
                  metadataStore,
                  blobStore,
                  TestTelemetry.DEFAULT,
                  OpenStreamInformation.DEFAULT,
                  executorService,
                  PhysicalIOConfiguration.DEFAULT),
              TestTelemetry.DEFAULT,
              logicalIOConfiguration,
              new ParquetColumnPrefetchStore(logicalIOConfiguration));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

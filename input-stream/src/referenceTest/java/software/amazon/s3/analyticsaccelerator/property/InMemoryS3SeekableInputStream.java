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
package software.amazon.s3.analyticsaccelerator.property;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import lombok.Getter;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.common.ConnectorConfiguration;
import software.amazon.s3.analyticsaccelerator.common.ObjectRange;
import software.amazon.s3.analyticsaccelerator.request.*;
import software.amazon.s3.analyticsaccelerator.util.OpenStreamInformation;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

public class InMemoryS3SeekableInputStream extends SeekableInputStream {

  @Getter private S3SeekableInputStreamFactory factory;
  private final SeekableInputStream delegate;

  /**
   * Returns an in memory S3 seekable stream. Pretends to point at s3://bucket/key and generates
   * `len` number of random bytes which will be then served back as object bytes.
   *
   * @param bucket the bucket
   * @param key the key
   * @param len the length of the data
   */
  public InMemoryS3SeekableInputStream(String bucket, String key, int len) throws IOException {
    S3URI s3URI = S3URI.of(bucket, key);
    ObjectClient objectClient = new InMemoryObjectClient(len);

    Map<String, String> configMap = new HashMap<>();

    configMap.put("physicalio.max.memory.limit", getMemoryCapacity());
    configMap.put("physicalio.memory.cleanup.frequency", "1");

    ConnectorConfiguration connectorConfig = new ConnectorConfiguration(configMap);
    S3SeekableInputStreamConfiguration config =
        S3SeekableInputStreamConfiguration.fromConfiguration(connectorConfig);

    factory = new S3SeekableInputStreamFactory(objectClient, config);
    this.delegate = factory.createStream(s3URI);
  }

  private static class InMemoryObjectClient implements ObjectClient {
    private static final String etag = "Random";
    private final int size;
    private byte[] content;

    public InMemoryObjectClient(int size) {
      this.size = size;
      this.content = new byte[size];

      // Fill with random bytes
      ThreadLocalRandom.current().nextBytes(this.content);
    }

    @Override
    public ObjectMetadata headObject(
        HeadRequest headRequest, OpenStreamInformation openStreamInformation) {
      return ObjectMetadata.builder().contentLength(size).etag(etag).build();
    }

    @Override
    public ObjectContent getObject(
        GetRequest getRequest, OpenStreamInformation openStreamInformation) {
      int start = 0;
      int end = size - 1;

      if (Objects.nonNull(getRequest.getRange())) {
        start = (int) getRequest.getRange().getStart();
        end = (int) getRequest.getRange().getEnd();
      }

      byte[] range = Arrays.copyOfRange(this.content, start, end + 1);
      return ObjectContent.builder().stream(new ByteArrayInputStream(range)).build();
    }

    @Override
    public void close() throws IOException {
      this.content = null;
    }
  }

  @Override
  public void seek(long pos) throws IOException {
    this.delegate.seek(pos);
  }

  @Override
  public long getPos() {
    return this.delegate.getPos();
  }

  @Override
  public int readTail(byte[] buf, int off, int n) throws IOException {
    return this.delegate.readTail(buf, off, n);
  }

  @Override
  public void readVectored(
      List<ObjectRange> ranges, IntFunction<ByteBuffer> allocate, Consumer<ByteBuffer> release)
      throws IOException {
    this.delegate.readVectored(ranges, allocate, release);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
    this.delegate.readFully(position, buffer, offset, length);
  }

  @Override
  public int read() throws IOException {
    return this.delegate.read();
  }

  private String getMemoryCapacity() {
    long maxHeapBytes = Runtime.getRuntime().maxMemory();
    double percentage = 0.01;
    long capacityBytes = (long) (maxHeapBytes * percentage);
    return String.valueOf(capacityBytes);
  }

  @Override
  public void close() throws IOException {
    this.delegate.close();
    if (factory != null) {
      factory.close();
    }
  }
}

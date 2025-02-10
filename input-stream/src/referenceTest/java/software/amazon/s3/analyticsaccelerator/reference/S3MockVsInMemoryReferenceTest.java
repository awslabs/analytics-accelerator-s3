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
package software.amazon.s3.analyticsaccelerator.reference;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.lifecycle.AfterContainer;
import net.jqwik.api.lifecycle.BeforeContainer;
import net.jqwik.testcontainers.Container;
import net.jqwik.testcontainers.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.utils.AttributeMap;
import software.amazon.awssdk.utils.IoUtils;
import software.amazon.s3.analyticsaccelerator.S3SdkObjectClient;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStream;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamConfiguration;
import software.amazon.s3.analyticsaccelerator.S3SeekableInputStreamFactory;
import software.amazon.s3.analyticsaccelerator.arbitraries.StreamArbitraries;
import software.amazon.s3.analyticsaccelerator.model.InMemorySeekableStream;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

@Testcontainers
public class S3MockVsInMemoryReferenceTest extends StreamArbitraries {

  private static final String S3MOCK_VERSION = "latest";
  private static final Collection<String> INITIAL_BUCKET_NAMES = asList("bucket");

  // Container will be started before each test method and stopped after
  @Container
  private static final S3MockContainer S3_MOCK =
      new S3MockContainer(S3MOCK_VERSION)
          .withInitialBuckets(String.join(",", INITIAL_BUCKET_NAMES));

  private static S3AsyncClient s3Client;
  private S3SeekableInputStream s3SeekableInputStream;
  private InMemorySeekableStream inMemorySeekableStream;
  private static final String TEST_BUCKET = "bucket";
  private static S3SeekableInputStreamFactory s3SeekableInputStreamFactory;

  @BeforeContainer
  static void setup() {
    s3Client = createS3ClientV2(S3_MOCK.getHttpsEndpoint());
    System.out.println("S3 client created successfully");
    // Initialise streams
    s3SeekableInputStreamFactory =
        new S3SeekableInputStreamFactory(
            new S3SdkObjectClient(s3Client), S3SeekableInputStreamConfiguration.DEFAULT);
  }

  @AfterContainer
  static void teardown() throws IOException {
    s3SeekableInputStreamFactory.close();
  }

  void setupStreams(int size) throws IOException {
    // Generate random data
    byte[] data = new byte[size];
    ThreadLocalRandom.current().nextBytes(data);

    // Put random data in S3
    String uuidKey = UUID.randomUUID().toString();
    S3URI uri = S3URI.of(TEST_BUCKET, uuidKey);

    s3Client
        .putObject(x -> x.bucket(TEST_BUCKET).key(uuidKey), AsyncRequestBody.fromBytes(data))
        .join();

    s3SeekableInputStream = s3SeekableInputStreamFactory.createStream(uri);

    inMemorySeekableStream = new InMemorySeekableStream(data);
  }

  /**
   * This test, while not a reference test, is useful for catching issues with the S3 Mock +
   * SeekableStream setup early. This test failing is a strong indication that something is majorly
   * wrong with the setup which is likely also breaking the other tests.
   */
  @Example
  public void regressionTest_S3MockInitThenSeekThenRead() throws IOException {
    setup();
    setupStreams(100);

    s3SeekableInputStream.seek(0);
    assertEquals(0, s3SeekableInputStream.getPos());
    s3SeekableInputStream.read();
  }

  @Property
  public void testPositionInitiallyZero(@ForAll("positiveStreamSizes") int size)
      throws IOException {
    setupStreams(size);

    assertEquals(0, inMemorySeekableStream.getPos());
    assertEquals(
        inMemorySeekableStream.getPos(),
        s3SeekableInputStream.getPos(),
        "getPos() of both implementations should match");
  }

  @Property
  public void testReadAndSeek_matchesWithReference(
      @ForAll("positiveStreamSizes") int size, @ForAll("validPositions") int pos)
      throws IOException {
    setupStreams(size);

    int validJump = pos % size;
    s3SeekableInputStream.seek(validJump);
    inMemorySeekableStream.seek(validJump);

    assertEquals(
        s3SeekableInputStream.getPos(),
        inMemorySeekableStream.getPos(),
        String.format("positions do not match after seeking to %s", validJump));
    assertEquals(
        s3SeekableInputStream.read(),
        inMemorySeekableStream.read(),
        String.format("returned data does not match after seeking to %s", validJump));
  }

  @Property
  public void testReadWithBuffer_matchesWithReference(
      @ForAll("positiveStreamSizes") int streamSize,
      @ForAll("validPositions") int pos,
      @ForAll("bufferSizes") int bufSize)
      throws IOException {
    setupStreams(streamSize);

    int validJump = pos % streamSize;
    s3SeekableInputStream.seek(validJump);
    inMemorySeekableStream.seek(validJump);

    assertEquals(
        s3SeekableInputStream.getPos(),
        inMemorySeekableStream.getPos(),
        String.format("positions do not match after seeking to %s", validJump));

    byte[] b1 = new byte[bufSize];
    byte[] b2 = new byte[bufSize];

    assertEquals(
        s3SeekableInputStream.read(b1, 0, bufSize),
        inMemorySeekableStream.read(b2, 0, bufSize),
        "number of bytes read should be the same");

    assertEquals(
        byteBufToString(b1),
        byteBufToString(b2),
        String.format("returned data does not match after requesting %s bytes", bufSize));

    assertEquals(
        s3SeekableInputStream.getPos(),
        inMemorySeekableStream.getPos(),
        String.format("positions do not match after reading"));

    assertEquals(
        s3SeekableInputStream.read(),
        inMemorySeekableStream.read(),
        String.format(
            "read() calls followed by a read(buff, off, len) do not return the same data"));
  }

  @Property
  public void testFullRead(@ForAll("positiveStreamSizes") int size) throws IOException {
    setupStreams(size);

    String seekableFullRead = IoUtils.toUtf8String(s3SeekableInputStream);
    String inMemoryFullRead = IoUtils.toUtf8String(inMemorySeekableStream);

    assertEquals(seekableFullRead, inMemoryFullRead);
  }

  @Property
  public void testTailReads(
      @ForAll("positiveStreamSizes") int streamSize,
      @ForAll("validPositions") int pos,
      @ForAll("bufferSizes") int bufSize)
      throws IOException {
    setupStreams(streamSize);

    int validJump = pos % streamSize;
    s3SeekableInputStream.seek(validJump);
    inMemorySeekableStream.seek(validJump);

    assertEquals(
        s3SeekableInputStream.getPos(),
        inMemorySeekableStream.getPos(),
        String.format("positions do not match after seeking to %s", validJump));

    int validBufSize = 1 + bufSize % streamSize;
    byte[] b1 = new byte[validBufSize];
    byte[] b2 = new byte[validBufSize];

    assertEquals(
        s3SeekableInputStream.readTail(b1, 0, validBufSize),
        inMemorySeekableStream.readTail(b2, 0, validBufSize),
        "number of bytes read from tail should be the same");

    assertEquals(
        byteBufToString(b1),
        byteBufToString(b2),
        String.format(
            "returned data does not match after requesting %s bytes from tail", validBufSize));

    assertEquals(
        s3SeekableInputStream.read(),
        inMemorySeekableStream.read(),
        String.format("read() calls followed by a readTail do not return the same data"));
  }

  private String byteBufToString(byte[] b) {
    return new String(b, StandardCharsets.UTF_8);
  }

  private static S3AsyncClient createS3ClientV2(String endpoint) {
    return S3AsyncClient.builder()
        .region(Region.of("us-east-1"))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
        // Path style access is a must because S3Mock cannot support virtual hosted style (as it
        // runs under localhost)
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        .endpointOverride(URI.create(endpoint))
        .httpClient(
            NettyNioAsyncHttpClient.builder()
                .buildWithDefaults(
                    AttributeMap.builder().put(TRUST_ALL_CERTIFICATES, Boolean.TRUE).build()))
        .build();
  }
}

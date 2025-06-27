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
package software.amazon.s3.analyticsaccelerator.benchmarks.data.generation;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import lombok.NonNull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.s3.analyticsaccelerator.access.S3ExecutionContext;
import software.amazon.s3.analyticsaccelerator.access.S3ObjectKind;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;
import software.amazon.s3.analyticsaccelerator.util.S3URI;

/**
 * Generates random data with a given size to test sequential reads . It uses {@link
 * S3TransferManager} backed by the CRT to upload data. It is able to generate objects of very large
 * sizes (GBs), as it doesn't materialize them in memory or local disk and uses the subscriber model
 * to upload data in chunks.
 */
public class RandomSequentialObjectGenerator extends BenchmarkObjectGenerator {
  /**
   * Creates an instance of random data generator
   *
   * @param context an instance of {@link S3ExecutionContext}
   * @param s3ObjectKind S3 Object Kind
   */
  public RandomSequentialObjectGenerator(
      @NonNull S3ExecutionContext context, @NonNull S3ObjectKind s3ObjectKind) {
    super(context, s3ObjectKind);
  }

  /**
   * Generate data
   *
   * @param s3URI S3 URI to generate data into
   * @param size object size
   */
  @Override
  public void generate(S3URI s3URI, long size) {
    int bufferSize =
        (int)
            Math.min(
                size,
                getContext()
                    .getConfiguration()
                    .getClientFactoryConfiguration()
                    .getCrtPartSizeInBytes());
    Preconditions.checkArgument(bufferSize > 0);

    String progressPrefix = createProgressPrefix(s3URI);
    System.out.println(
        progressPrefix + "Generating with " + size + " bytes [" + bufferSize + " bytes buffer]");

    performStreamUpload(
        s3URI,
        AsyncRequestBody.fromPublisher(new RandomDataGeneratorPublisher(size, bufferSize)),
        size);
  }

  @Override
  protected S3ObjectKind getEncryptedKind() {
    return S3ObjectKind.RANDOM_SEQUENTIAL_ENCRYPTED;
  }

  /** The publisher that produces random data */
  static class RandomDataGeneratorPublisher implements Publisher<ByteBuffer> {
    private final int bufferSize;
    private final AtomicLong bytesRemaining;

    /**
     * Creates a new instance of {@link RandomSequentialObjectGenerator}
     *
     * @param size object size
     * @param bufferSize buffer size
     */
    RandomDataGeneratorPublisher(long size, int bufferSize) {
      Preconditions.checkArgument(size > 0);
      Preconditions.checkArgument(bufferSize > 0);
      this.bufferSize = bufferSize;
      this.bytesRemaining = new AtomicLong(size);
    }
    /**
     * Called by the {@link S3TransferManager} to subscribe to new data. The model is pub-sub based;
     * {@link S3TransferManager} will call this and then will proceed pulling data from the
     * subscription.
     *
     * @param subscriber the {@link Subscriber} that will consume signals from this {@link
     *     Publisher}
     */
    @Override
    public void subscribe(@NonNull Subscriber<? super ByteBuffer> subscriber) {
      // Give subscriber a subscription to pull on
      subscriber.onSubscribe(
          new Subscription() {
            /**
             * {@link S3TransferManager} will call this to fill out the stream
             *
             * @param n the strictly positive number of elements to requests to the upstream {@link
             *     Publisher}
             */
            @Override
            public void request(long n) {
              // S3TransferManager requests one buffer at a time, and the code is written with this
              // assumption.
              Preconditions.checkArgument(n == 1);

              // We assume that we generate a chunk of size `bufferSize` here. We grab the next
              // "bufferSize" chunk
              long remaining = bytesRemaining.addAndGet(-1 * bufferSize);

              // Figure out how much we should fetch and whether we should complete. The easy case
              // where `remaining` > 0
              // This means we grabbed exactly `bufferSize` worth of data to allocate. If it turned
              // negative, this means that
              // we should generate less and the complete right after - we are at the end
              int currentBufferSize = bufferSize;
              boolean completed = false;
              if (remaining < 0) {
                completed = true;
                remaining = -1 * remaining;
                // We are buffer size past the end, this means a concurrent fetch completed the
                // buffer.
                // This should not happen in practice (S3TransferManager does this serially), but we
                // can handle it, so we do
                if (remaining >= bufferSize) {
                  remaining = -1;
                }
                currentBufferSize = (int) remaining;
              }

              // Allocate data and return it, if the math determined that we can
              if (currentBufferSize > 0) {
                // Generate random bytes into the buffer and return
                ByteBuffer byteBuffer = ByteBuffer.allocate(currentBufferSize);
                ThreadLocalRandom.current().nextBytes(byteBuffer.array());
                subscriber.onNext(byteBuffer);
              }

              // Completed if we are done
              if (completed) {
                subscriber.onComplete();
              }
            }

            /** Nothing to do here */
            @Override
            public void cancel() {}
          });
    }
  }
}

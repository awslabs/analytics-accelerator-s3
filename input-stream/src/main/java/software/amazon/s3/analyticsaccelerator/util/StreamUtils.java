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
package software.amazon.s3.analyticsaccelerator.util;

import static software.amazon.s3.analyticsaccelerator.util.Constants.ONE_KB;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3.analyticsaccelerator.request.ObjectContent;

/** Utility class for stream operations. */
public class StreamUtils {

  private static final int BUFFER_SIZE = 8 * ONE_KB;
  private static final Logger LOG = LoggerFactory.getLogger(StreamUtils.class);

  /**
   * Convert an InputStream from the underlying object to a byte array.
   *
   * @param objectContent the part of the object
   * @param timeoutMs read timeout in milliseconds
   * @return a byte array
   */
  public static byte[] toByteArray(ObjectContent objectContent, long timeoutMs)
      throws IOException, TimeoutException {
    InputStream inStream = objectContent.getStream();
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    byte[] buffer = new byte[BUFFER_SIZE];

    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Future<Void> future =
        executorService.submit(
            () -> {
              try {
                int numBytesRead;
                LOG.info("Starting to read from InputStream");
                while ((numBytesRead = inStream.read(buffer, 0, buffer.length)) != -1) {
                  outStream.write(buffer, 0, numBytesRead);
                }
                LOG.info("Successfully read from InputStream");
                return null;
              } finally {
                inStream.close();
              }
            });

    try {
      future.get(timeoutMs, TimeUnit.MILLISECONDS);

    } catch (TimeoutException e) {
      future.cancel(true);
      LOG.warn("Reading from InputStream has timed out.");
      throw new TimeoutException("Read operation timed out");
    } catch (Exception e) {
      throw new IOException("Error reading stream", e);
    } finally {
      executorService.shutdown();
    }

    return outStream.toByteArray();
  }
}

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

package software.amazon.s3.analyticsaccelerator.io.physical.data;

import lombok.NonNull;
import software.amazon.s3.analyticsaccelerator.util.BlockKey;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Block object stores the data of a stream
 */
public class DataBlock implements Closeable {
    private byte[] data;
    private final BlockKey blockKey;
    private final long generation;
    private final CountDownLatch dataReadyLatch = new CountDownLatch(1);

    /**
     * Constructs a DataBlock object
     *
     * @param blockKey the objectKey and range of the object
     * @param generation generation of the block in a sequential read pattern
     */
    public DataBlock(@NonNull BlockKey blockKey, long generation) {
        this.blockKey = blockKey;
        this.generation = generation;
    }

    @Override
    public void close() throws IOException {
        this.data = null;
    }
}

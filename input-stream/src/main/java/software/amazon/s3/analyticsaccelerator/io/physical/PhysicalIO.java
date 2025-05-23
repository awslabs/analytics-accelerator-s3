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
package software.amazon.s3.analyticsaccelerator.io.physical;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;
import software.amazon.s3.analyticsaccelerator.RandomAccessReadable;
import software.amazon.s3.analyticsaccelerator.common.ObjectRange;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlan;
import software.amazon.s3.analyticsaccelerator.io.physical.plan.IOPlanExecution;

/** An interface defining how a logical IO layer gets hooked into Physical IO. */
public interface PhysicalIO extends RandomAccessReadable {

  /**
   * Async method capable of executing a logical IO plan.
   *
   * @param ioPlan the plan to execute asynchronously
   * @return an IOPlanExecution object tracking the execution of the submitted plan
   */
  IOPlanExecution execute(IOPlan ioPlan) throws IOException;

  /**
   * Fetches the list of provided ranges in parallel. Byte buffers are created using the allocate
   * method, and may be direct or non-direct depending on the implementation of the allocate method.
   * When a provided range has been fully read, the associated future for it is completed.
   *
   * @param objectRanges Ranges to be fetched in parallel
   * @param allocate the function to allocate ByteBuffer
   * @throws IOException on any IO failure
   */
  void readVectored(List<ObjectRange> objectRanges, IntFunction<ByteBuffer> allocate)
      throws IOException;

  /**
   * Closes the PhysicalIO and optionally evicts associated data.
   *
   * @param shouldEvict whether associated data should be evicted
   * @throws IOException if an I/O error occurs
   */
  void close(boolean shouldEvict) throws IOException;
}

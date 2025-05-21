package software.amazon.s3.analyticsaccelerator.request;

import lombok.Value;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

@Value
public class ObjectRange {
    CompletableFuture<ByteBuffer> byteBufferCompletableFuture;
    long offset;
    int length;
}

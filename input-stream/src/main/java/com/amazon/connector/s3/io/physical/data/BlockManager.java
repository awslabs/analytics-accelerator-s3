package com.amazon.connector.s3.io.physical.data;

import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.common.telemetry.Operation;
import com.amazon.connector.s3.common.telemetry.Telemetry;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.io.physical.prefetcher.SequentialPatternDetector;
import com.amazon.connector.s3.io.physical.prefetcher.SequentialReadProgression;
import com.amazon.connector.s3.request.ObjectClient;
import com.amazon.connector.s3.request.Range;
import com.amazon.connector.s3.request.ReadMode;
import com.amazon.connector.s3.util.S3URI;
import com.amazon.connector.s3.util.StreamAttributes;
import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import lombok.NonNull;

/** Implements a Block Manager responsible for planning and scheduling reads on a key. */
public class BlockManager implements Closeable {
  private final S3URI s3URI;
  private final MetadataStore metadataStore;
  private final BlockStore blockStore;
  private final ObjectClient objectClient;
  private final Telemetry telemetry;
  private final SequentialPatternDetector patternDetector;
  private final SequentialReadProgression sequentialReadProgression;
  private final IOPlanner ioPlanner;
  private final PhysicalIOConfiguration configuration;
  private final RangeOptimiser rangeOptimiser;

  private static final String OPERATION_MAKE_RANGE_AVAILABLE = "block.manager.make.range.available";

  /**
   * Constructs a new BlockManager.
   *
   * @param s3URI the S3 URI of the object
   * @param objectClient object client capable of interacting with the underlying object store
   * @param telemetry an instance of {@link Telemetry} to use
   * @param metadataStore the metadata cache
   * @param configuration the physicalIO configuration
   * @param memoryTracker the memory tracker
   */
  public BlockManager(
      @NonNull S3URI s3URI,
      @NonNull ObjectClient objectClient,
      @NonNull MetadataStore metadataStore,
      @NonNull Telemetry telemetry,
      @NonNull PhysicalIOConfiguration configuration,
      @NonNull MemoryTracker memoryTracker) {
    this.s3URI = s3URI;
    this.objectClient = objectClient;
    this.metadataStore = metadataStore;
    this.telemetry = telemetry;
    this.configuration = configuration;
    this.blockStore = new BlockStore(s3URI, metadataStore, configuration, memoryTracker);
    this.patternDetector = new SequentialPatternDetector(blockStore);
    this.sequentialReadProgression = new SequentialReadProgression();
    this.ioPlanner = new IOPlanner(blockStore);
    this.rangeOptimiser = new RangeOptimiser(configuration);
  }

  /**
   * Given the position of a byte, return the block holding it.
   *
   * @param pos the position of a byte
   * @return the Block holding the byte or empty if the byte is not in the BlockStore
   */
  public synchronized Optional<Block> getBlock(long pos) {
    return this.blockStore.getBlock(pos);
  }

  /**
   * Make sure that the byte at a give position is in the BlockStore.
   *
   * @param pos the position of the byte
   * @param readMode whether this ask corresponds to a sync or async read
   */
  public synchronized void makePositionAvailable(long pos, ReadMode readMode) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    // Position is already available --> return corresponding block
    if (getBlock(pos).isPresent()) {
      return;
    }

    makeRangeAvailable(pos, 1, readMode);
  }

  private boolean isRangeAvailable(long pos, long len) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");

    long lastByteOfRange = pos + len - 1;

    OptionalLong nextMissingByte = blockStore.findNextMissingByte(pos);
    if (nextMissingByte.isPresent()) {
      return lastByteOfRange < nextMissingByte.getAsLong();
    }

    // If there is no missing byte after pos, then the whole object is already fetched
    return true;
  }

  /**
   * Method that ensures that a range is fully available in the object store. After calling this
   * method the BlockStore should contain all bytes in the range and we should be able to service a
   * read through the BlockStore.
   *
   * @param pos start of a read
   * @param len length of the read
   * @param readMode whether this ask corresponds to a sync or async read
   */
  public synchronized void makeRangeAvailable(long pos, long len, ReadMode readMode) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");
    Preconditions.checkArgument(0 <= len, "`len` must not be negative");

    if (isRangeAvailable(pos, len)) {
      return;
    }

    // In case of a sequential reading pattern, calculate the generation and adjust the requested
    // effectiveEnd of the requested range
    long effectiveEnd = pos + Math.max(len, configuration.getReadAheadBytes()) - 1;

    // Check sequential prefetching
    final long generation;
    if (patternDetector.isSequentialRead(pos)) {
      generation = patternDetector.getGeneration(pos);
      effectiveEnd = truncatePos(pos + sequentialReadProgression.getSizeForGeneration(generation));
    } else {
      generation = 0;
    }

    // Fix "effectiveEnd", so we can pass it into the lambda
    final long effectiveEndFinal = effectiveEnd;
    this.telemetry.measureStandard(
        () ->
            Operation.builder()
                .name(OPERATION_MAKE_RANGE_AVAILABLE)
                .attribute(StreamAttributes.uri(this.s3URI))
                .attribute(StreamAttributes.range(pos, pos + len - 1))
                .attribute(StreamAttributes.effectiveRange(pos, pos + effectiveEndFinal - 1))
                .attribute(StreamAttributes.generation(generation))
                .build(),
        () -> {
          // Determine the missing ranges and fetch them
          List<Range> missingRanges =
              ioPlanner.planRead(pos, effectiveEndFinal, getLastObjectByte());
          List<Range> splits = rangeOptimiser.splitRanges(missingRanges);
          splits.forEach(
              r -> {
                Block block =
                    new Block(
                        s3URI,
                        objectClient,
                        telemetry,
                        r.getStart(),
                        r.getEnd(),
                        generation,
                        readMode);
                blockStore.add(block);
              });
        });
  }

  private long getLastObjectByte() {
    return this.metadataStore.get(s3URI).getContentLength() - 1;
  }

  private long truncatePos(long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    return Math.min(pos, getLastObjectByte());
  }

  /** Closes the {@link BlockManager} and frees up all resources it holds */
  @Override
  public void close() {
    blockStore.close();
  }
}

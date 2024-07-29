package com.amazon.connector.s3.io.physical.data;

import com.amazon.connector.s3.ObjectClient;
import com.amazon.connector.s3.common.Preconditions;
import com.amazon.connector.s3.io.physical.PhysicalIOConfiguration;
import com.amazon.connector.s3.io.physical.plan.Range;
import com.amazon.connector.s3.io.physical.prefetcher.SequentialPatternDetector;
import com.amazon.connector.s3.io.physical.prefetcher.SequentialReadProgression;
import com.amazon.connector.s3.request.ReadMode;
import com.amazon.connector.s3.util.S3URI;
import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/** Implements a Block Manager responsible for planning and scheduling reads on a key. */
public class BlockManager implements Closeable {

  private final S3URI s3URI;
  private final MetadataStore metadataStore;
  private final BlockStore blockStore;
  private final ObjectClient objectClient;
  private final SequentialPatternDetector patternDetector;
  private final SequentialReadProgression sequentialReadProgression;
  private final IOPlanner ioPlanner;
  private final PhysicalIOConfiguration configuration;
  private final RangeOptimiser rangeOptimiser;

  /**
   * Constructs a new BlockManager.
   *
   * @param s3URI the S3 URI of the object
   * @param objectClient object client capable of interacting with the underlying object store
   * @param metadataStore the metadata cache
   * @param configuration the physicalIO configuration
   */
  public BlockManager(
      S3URI s3URI,
      ObjectClient objectClient,
      MetadataStore metadataStore,
      PhysicalIOConfiguration configuration) {
    Preconditions.checkNotNull(s3URI, "`s3URI` must not be null");
    Preconditions.checkNotNull(objectClient, "`objectClient` must not be null");
    Preconditions.checkNotNull(metadataStore, "`metadataStore` must not be null");
    Preconditions.checkNotNull(configuration, "`configuration` must not be null");

    this.s3URI = s3URI;
    this.objectClient = objectClient;
    this.metadataStore = metadataStore;
    this.configuration = configuration;
    this.blockStore = new BlockStore(s3URI, metadataStore);
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

    len = Math.max(len, configuration.getReadAheadBytes());

    // In case of a sequential reading pattern, calculate the generation and adjust the requested
    // end of the requested range
    long end = pos + len - 1;
    final long generation;

    if (patternDetector.isSequentialRead(pos)) {
      generation = patternDetector.getGeneration(pos);
      long newSize = sequentialReadProgression.getSizeForGeneration(generation);
      end = truncatePos(pos + newSize);
    } else {
      generation = 0;
    }

    // Determine the missing ranges and fetch them
    List<Range> missingRanges = ioPlanner.planRead(pos, end, getLastObjectByte());
    List<Range> splits = rangeOptimiser.splitRanges(missingRanges);
    splits.forEach(
        r -> {
          Block block =
              new Block(s3URI, objectClient, r.getStart(), r.getEnd(), generation, readMode);
          blockStore.add(block);
        });
  }

  private long getLastObjectByte() {
    return this.metadataStore.get(s3URI).join().getContentLength() - 1;
  }

  private long truncatePos(long pos) {
    Preconditions.checkArgument(0 <= pos, "`pos` must not be negative");

    return Math.min(pos, getLastObjectByte());
  }

  @Override
  public void close() {
    blockStore.close();
  }
}

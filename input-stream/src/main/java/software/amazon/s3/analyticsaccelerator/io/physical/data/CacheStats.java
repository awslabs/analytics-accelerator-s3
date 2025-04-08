package software.amazon.s3.analyticsaccelerator.io.physical.data;

import java.util.concurrent.atomic.AtomicLong;

public class CacheStats {
    private static final AtomicLong cacheHits = new AtomicLong(0);
    private static final AtomicLong cacheMisses = new AtomicLong(0);

    public static void recordHit() {
        cacheHits.incrementAndGet();
    }

    public static void recordMiss() {
        cacheMisses.incrementAndGet();
    }

    public static long getHits() {
        return cacheHits.get();
    }

    public static long getMisses() {
        return cacheMisses.get();
    }

    public static double getHitRate() {
        long hits = cacheHits.get();
        long total = hits + cacheMisses.get();
        return total == 0 ? 0 : (double) hits / total;
    }

    public static void resetStats() {
        cacheHits.set(0);
        cacheMisses.set(0);
    }

    public static String getStats() {
        return String.format("Cache Hits: %d, Misses: %d, Hit Rate: %.2f%%",
                getHits(), getMisses(), getHitRate() * 100);
    }
}
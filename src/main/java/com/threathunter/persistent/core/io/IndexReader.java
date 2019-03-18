package com.threathunter.persistent.core.io;

import java.util.Map;

/**
 * 
 */
public interface IndexReader {
    /**
     * Every index value is contains 5 bytes for a shard.
     * one byte for shard number, 4 bytes for offset
     * in that shard.
     * And key bytes is grouped by every 10 seconds.
     * @return every shard with a map of 10-seconds grouped
     * timestamp and offset.
     */
    Map<Integer, Map<Long, Integer>> getOffsets(byte[] key);
}

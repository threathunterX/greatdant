package com.threathunter.persistent.core.io;

/**
 * 
 */
public interface IndexWriter {

    boolean writeIndex(byte[] key, Long offset, int shard, long timestamp);

    boolean writeIndex(byte[] key, Long offset, int shard);
}

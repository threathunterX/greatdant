package com.threathunter.persistent.core.io;

/**
 * Created by daisy on 16-4-7.
 */
public interface IndexWriter {

    boolean writeIndex(byte[] key, Long offset, int shard, long timestamp);

    boolean writeIndex(byte[] key, Long offset, int shard);
}

package com.threathunter.persistent.core.io;


import com.threathunter.persistent.core.util.BytesEncoderDecoder;
import com.threathunter.persistent.core.EventPersistCommon;
import com.threathunter.persistent.core.util.ConstantsUtil;
import com.threathunter.persistent.core.util.SystemClock;
import com.google.common.primitives.Ints;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

import static org.iq80.leveldb.impl.Iq80DBFactory.factory;

/**
 * 
 *
 * In implementation, we will split index file per hour, because leveldb can just
 * allow one process to access the db at a time.
 * We do not need to expire keys manually, cause indices is split into so many
 * files according to timestamp, we can just use a script to delete useless indices
 *
 * This writer is not thread safe and should be used in one single thread.
 */
public class LevelDbIndexReadWriter implements IndexWriter, IndexReader, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LevelDbIndexReadWriter.class);
    private static final LevelDbIndexReadWriter INSTANCE = new LevelDbIndexReadWriter();

    private static final Charset CS = Charset.forName("ISO-8859-1");

    // currently the directory and interval can not be modified dynamically
    private volatile String currentPath = "";
    // these data is not thread safe

    // the current leveldb file
    private volatile DB db;
    // the current time slot for write, we write index every 10 seconds
    private volatile long currentTimestampSlot = -1;

    private volatile Map<String, Map<Integer, Long>> cachingData = new HashMap<>();

    private volatile boolean running = false;
    private final Object lock = new Object();

    private LevelDbIndexReadWriter() {}

    public LevelDbIndexReadWriter(final String initialPath) {
        running = true;
        updateIndexDir(initialPath);
    }

    public static LevelDbIndexReadWriter getInstance() {
        return INSTANCE;
    }

    public void initial(final String newPath) {
        running = true;
        updateIndexDir(newPath);
    }

    @Override
    public void close() {
        LOGGER.warn("final close");

        synchronized (lock) {
            this.closeIndexFile(this.db, currentPath);
            this.currentTimestampSlot = -1;
            this.db = null;
            running = false;
        }
    }

    public boolean flushCache() {
        if (this.db == null) {
            return false;
        }

        if (this.cachingData.isEmpty()) {
            return true;
        }

        WriteBatch rb = null;
        try {
            rb = this.db.createWriteBatch();
            for (Map.Entry<String, Map<Integer, Long>> entry : this.cachingData.entrySet()) {
                byte[] key = entry.getKey().getBytes(CS);
                rb.put(getLevelDBKey(key, currentTimestampSlot), getLevelDBValue(entry.getValue()));
            }
            db.write(rb);
        } catch (Exception ex) {
            LOGGER.error("fail to write a batch of index information", ex);
            return false;
        } finally {
            this.cachingData = new HashMap<>();
            if (rb != null) {
                try {
                    rb.close();
                } catch (IOException ignore) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Will check if need to change to new db path,
     * this will not really write into leveldb, but put to cache map,
     * and the map will cache the first if the key is the same
     * @param key
     * @param offset
     * @param timestamp
     * @return
     */
    @Override
    public boolean writeIndex(byte[] key, Long offset, int shard, long timestamp) {
        if (timestamp < currentTimestampSlot)
            return false;

        if (!checkForFlushing(timestamp)) {
            LOGGER.error("fail to flush cache");
        }

        if (this.db == null) {
            return false;
        }

        String formatedKey = getISO8859String(key);
        if (!this.cachingData.containsKey(formatedKey)) {
            this.cachingData.put(formatedKey, new HashMap<>());
        }
        if (!this.cachingData.get(formatedKey).containsKey(shard)) {
            this.cachingData.get(formatedKey).put(shard, offset);
        }

        return true;
    }

    /**
     * Will check if need to change to new db path,
     * this will not really write into leveldb, but put to cache map,
     * and the map will cache the first if the key is the same
     * @param key
     * @param offset
     * @return
     */
    @Override
    public boolean writeIndex(byte[] key, Long offset, int shard) {
        return this.writeIndex(key, offset, shard, SystemClock.getCurrentTimestamp());
    }

    public boolean updateIndexDir(String newDir) {
        if (this.db != null) {
            flushCache();
            final DB oldDB = this.db;
            final String oldPath = this.currentPath;
            new Thread(() -> closeIndexFile(oldDB, oldPath)).start();
        }

        // build the new db
        try {
            openNewLogIndexFile(newDir);
        } catch (Exception e) {
            LOGGER.error("failed to update to new log path", e);
            return false;
        }
        this.currentPath = newDir;
        return true;
    }

    @Override
    public Map<Integer, Map<Long, Integer>> getOffsets(byte[] key) {
        return getOffsets(key, currentTimestampSlot);
    }

    public Map<Integer, Map<Long, Integer>> getOffsets(byte[] key, long currentTimestamp) {
        long hourInMillis = currentTimestamp / 3600000 * 3600000;
        Map<Integer, Map<Long, Integer>> shardMap = new HashMap<>();
        for (int i = 0; i < 3600000; i += 10000) {
            byte[] indexKey = getLevelDBKey(key, hourInMillis + i);
            byte[] result = this.db.get(indexKey);
            if (result == null) {
                continue;
            }
            int pos = 0;
            byte b = 0;
            while (pos < result.length) {
                Integer shard = Ints.fromBytes(b, b, b, result[pos]);
                if (!shardMap.containsKey(shard)) {
                    shardMap.put(shard, new HashMap<>());
                }
                Integer offset = Ints.fromBytes(result[pos + 1], result[pos + 2],
                        result[pos + 3], result[pos + 4]);
                shardMap.get(shard).put(hourInMillis + i, offset);
                pos += 5;
            }
        }
        return shardMap;
    }

    private void openNewLogIndexFile(final String newPath) {
        DB result;
        String tempFileName = String.format("%s/%s.%s", newPath, ConstantsUtil.PERSISTENT_INDEX_DIR, ConstantsUtil.PERSISTENT_TEMP_SUFFIX);
        File tempFile = new File(tempFileName);
        String fileName = String.format("%s/%s", newPath, ConstantsUtil.PERSISTENT_INDEX_DIR);
        File file = new File(fileName);

        if (tempFile.exists()) {
            LOGGER.error("temperate index file {} has already existed, try use it", tempFileName);
        }

        synchronized (lock) {
            if (!running)
                return;

            if (file.exists()) {
                // there is an old data base, try to use that one
                // 1. first move the the temporate file
                try {
                    Files.move(file.toPath(), tempFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
                    LOGGER.info("successfully renamed the index db {} to temperate file {}", fileName, tempFileName);
                } catch (IOException e) {
                    LOGGER.error("fail to rename to the temperate file " + tempFileName, e);
                    throw new RuntimeException(e);
                }
            }

            result = openLevelDB(tempFile);
            this.currentTimestampSlot = SystemClock.getCurrentTimestamp() / 1000 / 10 * 10 * 1000;
            this.db = result;
            LOGGER.info("successfully create or restore index file {}", tempFileName);
        }
    }

    private void closeDb(DB db) {
        flushCache();
        if (db != null) {
            try {
                db.close();
            } catch (IOException e) {
                LOGGER.error("closeDb index db failed", e);
            }
        }
    }

    private String getISO8859String(byte[] str) {
        try {
            return new String(str, "ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("no iso-8859-1 encoder");
        }
    }

    /**
     * Get the formatted key from key and timestamp.
     * Key can be an ip, or some other.
     * <p/>
     * The final key for an index will be byte[] length 6,
     * first byte is for the version, 1 is for keytype from parameter. Next 3 bytes will be the first 3 bytes[]
     * from the key. The last is 2 bytes to record the time will an hour, grouped by 10s.
     *
     * @param typedKey
     * @param timeInMillis
     * @return
     */
    private byte[] getLevelDBKey(byte[] typedKey, Long timeInMillis) {
        byte[] result = new byte[2 + EventPersistCommon.INDEX_KEY_SIZE];
        System.arraycopy(typedKey, 0, result, 0, EventPersistCommon.INDEX_KEY_SIZE);

        long ts = ((timeInMillis / 1000) % 3600) / 10;
        System.arraycopy(BytesEncoderDecoder.encode32(ts), 2, result, 4, 2);
        return result;
    }

    /**
     * Get the byte[] for an offset to be the index value, record a log's location of a log file.
     * @param
     * @return
     */
    private byte[] getLevelDBValue(Map<Integer, Long> shardOffset) {
        byte[] value = new byte[shardOffset.size() * 5];
        ByteBuffer target = ByteBuffer.wrap(value);
        for (Map.Entry<Integer, Long> entry : shardOffset.entrySet()) {
            target.put(entry.getKey().byteValue());
            target.put(BytesEncoderDecoder.encode64(entry.getValue()), 4, 4);
        }
        return target.array();
    }

    /**
     * Open a leveldb, no more
     * @param f
     * @return
     */
    private DB openLevelDB(File f) {
        Options options = new Options();
        options.createIfMissing(true);
        try {
            return factory.open(f, options);
        } catch (IOException e) {
            LOGGER.error("file to open db " + f.getName(), e);
            throw new RuntimeException(e);
        }
    }

    private boolean checkForFlushing(long currentTimeMillis) {
        if (currentTimeMillis / 1000 / 10 > this.currentTimestampSlot / 1000 / 10)  {
            // do flushing
            boolean result = flushCache();
            this.currentTimestampSlot = currentTimeMillis / 1000 / 10 * 10 * 1000;
            return result;
        } else {
            return true;
        }
    }

    private void closeIndexFile(final DB olddb, String oldPath) {
        if (olddb == null) {
            return;
        }

        closeDb(olddb);

        // rename the temperate file for regular usage
        String tempFileName = String.format("%s/%s.%s", oldPath, ConstantsUtil.PERSISTENT_INDEX_DIR, ConstantsUtil.PERSISTENT_TEMP_SUFFIX);
        File tempFile = new File(tempFileName);
        String fileName = String.format("%s/%s", oldPath, ConstantsUtil.PERSISTENT_INDEX_DIR);
        File file = new File(fileName);
        LOGGER.info("closeing index file {}, the last time slot is {}", tempFileName, currentTimestampSlot);
        if (tempFile.exists()) {
            try {
                Object p = Files.move(tempFile.toPath(), file.toPath(), StandardCopyOption.ATOMIC_MOVE);
                LOGGER.info("successfully renamed the index db {}, {}", tempFileName, p);
            } catch (IOException e) {
                LOGGER.error("fail to rename the temperate file " + tempFileName, e);
            }
        } else  {
            LOGGER.error("no index with name {} is found when closing the old index file", tempFileName);
        }
    }
}

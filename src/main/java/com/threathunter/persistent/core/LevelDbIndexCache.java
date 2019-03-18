package com.threathunter.persistent.core;

import com.threathunter.common.ShutdownHookManager;
import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.persistent.core.io.LevelDbIndexReadWriter;
import com.threathunter.persistent.core.util.ConstantsUtil;
import com.threathunter.persistent.core.util.PathHelper;
import com.threathunter.persistent.core.util.SystemClock;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 
 */
public class LevelDbIndexCache {
    private static final Logger LOGGER = LoggerFactory.getLogger(LevelDbIndexCache.class);
    private static final LevelDbIndexCache INSTANCE = new LevelDbIndexCache();

    private final String persistPath = CommonDynamicConfig.getInstance().getString("persist_path", PathHelper.getModulePath() + "/persistent");
    private final Cache<Long, LevelDbIndexReadWriter> levelDbIndexReadWriterCache;

    private LevelDbIndexCache() {
        this.levelDbIndexReadWriterCache = CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).removalListener((notification) -> {
            LevelDbIndexReadWriter instance = (LevelDbIndexReadWriter) notification.getValue();
            instance.close();
        }).build();

        ShutdownHookManager.get().addShutdownHook(() -> this.levelDbIndexReadWriterCache.invalidateAll(), 100);
    }

    public static LevelDbIndexCache getInstance() {
        return INSTANCE;
    }

    public void close() {
        this.levelDbIndexReadWriterCache.invalidateAll();
    }

    public LevelDbIndexReadWriter getIndexReadWriter(final long hourTimeMillis) {
        if (hourTimeMillis == SystemClock.getCurrentTimestamp() / ConstantsUtil.HOUR_MILLIS * ConstantsUtil.HOUR_MILLIS) {
            return LevelDbIndexReadWriter.getInstance();
        }
        LevelDbIndexReadWriter readWriter = null;
        try {
            readWriter = levelDbIndexReadWriterCache.get(hourTimeMillis, () -> {
                String levelDbPath = String.format("%s/%s", this.persistPath, new DateTime(hourTimeMillis).toString("yyyyMMddHH"));
                return new LevelDbIndexReadWriter(levelDbPath);
            });
        } catch (Exception e) {
            LOGGER.error("error when open leveldb dir");
        }
        return readWriter;
    }
}

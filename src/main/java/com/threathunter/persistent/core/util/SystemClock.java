package com.threathunter.persistent.core.util;

import com.threathunter.common.ShutdownHookManager;
import com.threathunter.config.CommonDynamicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by daisy on 17/1/11.
 */
public class SystemClock {
    private static final Logger LOGGER = LoggerFactory.getLogger(SystemClock.class);
    private static final boolean userCustomerClock;
    private static final TimeSyncThread syncThread;

    static {
        userCustomerClock = CommonDynamicConfig.getInstance().getBoolean(ConstantsUtil.CUSTOMER_CLOCK, false);
        if (userCustomerClock) {
            LOGGER.warn("using customer clock, please make sure to sync timestamp correctly");
            syncThread = new TimeSyncThread();
            syncThread.start();
            ShutdownHookManager.get().addShutdownHook(() -> {
                syncThread.running = false;
                try {
                    syncThread.join(1000);
                } catch (Exception e) {
                    LOGGER.error("error when stop customer sync timestamp thread", e);
                }
            }, 100);
        } else {
            syncThread = null;
        }
    }

    public static void syncCustomerTimestamp(long customerTimestamp) {
        if (userCustomerClock) {
            syncThread.syncTimestamp(customerTimestamp);
        }
    }

    public static long getCurrentTimestamp() {
        if (userCustomerClock) {
            return syncThread.getSyncedTimestamp();
        }
        return System.currentTimeMillis();
    }

    private static class TimeSyncThread extends Thread {
        private volatile long syncedTimestamp = -1;
        private volatile boolean running = false;

        public void syncTimestamp(long currentTimestamp) {
            this.syncedTimestamp = currentTimestamp;
        }

        public long getSyncedTimestamp() {
            return this.syncedTimestamp;
        }

        @Override
        public void run() {
            if (running) {
                LOGGER.warn("customer sync timestamp thread already running");
            }
            running = true;
            while (true) {
                try {
                    Thread.sleep(100);
                    this.syncedTimestamp += 100;
                } catch (InterruptedException e) {
                    LOGGER.error("customer sync timestamp thread interrupted", e);
                }
            }
        }
    }
}

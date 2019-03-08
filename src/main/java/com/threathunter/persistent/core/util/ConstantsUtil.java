package com.threathunter.persistent.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 * Created by yy on 17-10-12.
 */
public class ConstantsUtil {
    private static Logger logger= LoggerFactory.getLogger(ConstantsUtil.class);
    public static final long HOUR_MILLIS = 1000 * 60 * 60;
    public static final String PERSISTENT_SHARD_CONFIG_NAME = "nebula.persistent.log.shard";
    public static final String PERSISTENT_DIR_CONFIG_NAME = "persist_path";
    public static final String PERSISTENT_DIR = "persistent";
    public static final String PERSISTENT_EVENT_DIR = "log";
    public static final String PERSISTENT_TEMP_SUFFIX = "tmp";
    public static final byte CHECK_CODE = (byte) 0xcc;
    public static final String CUSTOMER_CLOCK = "customer_clock";
    public static final String EVENTS_SCHEMA="events_schema.json";
//    public static final String EVENT_ENTRY="^EVENT_ENTRY^";
    public static final String PERSISTENT_INDEX_DIR = "index";
//    public static byte[] getEventEntryBytes(){
//        try {
//            return EVENT_ENTRY.getBytes("utf-8");
//        } catch (UnsupportedEncodingException e) {
//            logger.error("ConstantsUtil can not get event entry literal bytes(^EVENT_ENTRY^)",e);
//            throw new RuntimeException();
//        }
//    }
}

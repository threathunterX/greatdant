package com.threathunter.persistent.core.util;

import com.threathunter.config.CommonDynamicConfig;
import com.threathunter.persistent.core.EventPersistCommon;
import com.threathunter.persistent.core.io.ShardFile;
import com.threathunter.persistent.core.io.BufferedRandomAccessFile;
import com.threathunter.persistent.core.io.QueryCSVReader;
import com.threathunter.persistent.core.io.QueryCSVWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 
 */
public class IOUtil {

    private static String DEFAULT_DIR;
    private static String DEFAULT_PERSISTENT_DIR;
    private static String DEFAULT_RESOURCE_DIR;
    private static Logger logger = LoggerFactory.getLogger(IOUtil.class);

    static {
        DEFAULT_PERSISTENT_DIR = CommonDynamicConfig.getInstance()
                .getString(ConstantsUtil.PERSISTENT_DIR_CONFIG_NAME,
                        PathHelper.getModulePath() + "/" + ConstantsUtil.PERSISTENT_DIR);
        DEFAULT_DIR = CommonDynamicConfig.getInstance()
                .getString("events_query_result_dir", PathHelper.getModulePath() + "/" + "query");
        DEFAULT_RESOURCE_DIR = CommonDynamicConfig.getInstance().getString("events_query_resources_dir",
                PathHelper.getModulePath() + "/" + "resources");
        EventPersistCommon.ensure_dir(DEFAULT_DIR);
        logger.warn("query dir is {}", DEFAULT_DIR);
        ;
//    DEFAULT_PERSISTENT_DIR = PathHelper.getModulePath() + "/" + ConstantsUtil.PERSISTENT_DIR;
//    DEFAULT_DIR = PathHelper.getModulePath()+"/" + "query";
//    DEFAULT_RESOURCE_DIR = PathHelper.getModulePath() + "/" + "resources";
    }


    public static String generateFileName(final String requestId) {
        return String.format("events_query_%s.csv", requestId);
    }

    public static QueryCSVWriter createWriter(final String name, final List<String> showCols)
            throws IOException {
        return createWriter(name, showCols, DEFAULT_DIR);
    }

    public static QueryCSVWriter createWriter(final String name, final List<String> showCols,
                                              String fileDir)
            throws IOException {
        String filePath = String.format("%s/%s", fileDir, name);

        return new QueryCSVWriter(filePath, showCols);
    }

    public static String getTotalFileName(final String requestId) {
        return String.format("events_query_total_%s", requestId);
    }

    public static String getPath(final String name) {
        return String.format("%s/%s", DEFAULT_DIR, name);
    }

    public static String getQueryTotalPath(final String name) {
        return String.format("%s/events_query_total_%s.csv", DEFAULT_DIR, name);
    }

    public static void writeTotal(String id, int total) {
        String path = getQueryTotalPath(id);
        try {
            FileWriter writer = new FileWriter(path);
            writer.write(String.valueOf(total));
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<Map<String, Object>> getQueryResultFromFile(final String fileName,
                                                                   final int page) {
        try {
            QueryCSVReader reader = new QueryCSVReader(String.format("%s/%s", DEFAULT_DIR, fileName));
            return reader.readLines(page);
        } catch (Exception e) {
            logger.error("read from csv error", e);
            return null;
        }
    }

    public static String getDefaultDir() {
        return DEFAULT_DIR;
    }

    public static String getDefaultPersistentDir() {
        return DEFAULT_PERSISTENT_DIR;
    }

    public static String getDefaultResourceDir() {
        return DEFAULT_RESOURCE_DIR;
    }

    public static List<Map<String, Object>> getQueryResultFromFile(String fileName, int page, int pageSize) {
        try {
            QueryCSVReader reader = new QueryCSVReader(String.format("%s/%s", DEFAULT_DIR, fileName));
            return reader.readLines(page, pageSize);
        } catch (Exception e) {
            logger.error("read from csv error", e);
            return null;
        }
    }

    public static String getHourDirectory(Long timestamp) {
        return null;
    }

    public static ShardFile[] createShardFiles(String dir) {
        ShardFile[] files = new ShardFile[16];
        for (int i = 0; i < 16; i++) {
            try {
                files[i] = new ShardFile(String.valueOf(i), dir);

            } catch (IOException exception) {

            }
        }
        return files;
    }

    public static BufferedRandomAccessFile createBAFile(String name, String dir)
            throws IOException {
        String tmpFileName = String.format("%s/%s/%s.%s/%s", DEFAULT_PERSISTENT_DIR, dir, ConstantsUtil.PERSISTENT_EVENT_DIR,
                ConstantsUtil.PERSISTENT_TEMP_SUFFIX, name);

        BufferedRandomAccessFile file;

        if (!new File(tmpFileName).exists()) {
            file = new BufferedRandomAccessFile(
                    String.format("%s/%s/%s/%s", DEFAULT_PERSISTENT_DIR, dir, ConstantsUtil.PERSISTENT_EVENT_DIR, name), "r");
        } else {
            file = new BufferedRandomAccessFile(tmpFileName, "r");
        }

        if(file==null )
            throw new IllegalStateException("file should be exist");
        return file;
    }

    public static String getPathOfEventsSchema(String dir) {
        String tmpFileName = String.format("%s/%s/%s", DEFAULT_PERSISTENT_DIR, dir, ConstantsUtil.EVENTS_SCHEMA);
         return tmpFileName;
    }
}

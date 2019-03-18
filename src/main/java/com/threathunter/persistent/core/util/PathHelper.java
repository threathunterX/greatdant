package com.threathunter.persistent.core.util;

import org.joda.time.DateTime;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 
 */
public class PathHelper {
    public static String getModulePath() {
        return System.getProperties().get("user.dir").toString();
    }

    public static List<String> getDirNamesFromTimestamp(Long start, Long end) {
        Set<String> dirs = new LinkedHashSet<>();
        Long startPoint = start / ConstantsUtil.HOUR_MILLIS * ConstantsUtil.HOUR_MILLIS;
        while (startPoint < end) {
            String hourDir = new DateTime(startPoint).toString("yyyyMMddHH");
            dirs.add(hourDir);
            startPoint = startPoint + ConstantsUtil.HOUR_MILLIS;
        }
        Long endPoint = end / ConstantsUtil.HOUR_MILLIS * ConstantsUtil.HOUR_MILLIS;
        if (end >= endPoint) {
            String hourDir = new DateTime(endPoint).toString("yyyyMMddHH");
            dirs.add(hourDir);
        }
        return new ArrayList<>(dirs);
    }

    public static boolean ifDirExist(String dir) {
        String path = String.format("%s/%s", IOUtil.getDefaultPersistentDir(), dir);
        File file = new File(path);
        if (file.exists() && file.isDirectory())
            return true;
        return false;
    }
}

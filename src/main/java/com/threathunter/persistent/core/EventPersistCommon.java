package com.threathunter.persistent.core;


import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static com.threathunter.common.Utility.isEmptyStr;

/**
 * Common util in event log module.
 *
 * @author Wen Lu
 */
public class EventPersistCommon {
    private static final Logger logger = LoggerFactory.getLogger(EventPersistCommon.class);

    private static final Map<String, Byte> keyType = new HashMap<>();

    static {
        keyType.put("c_ip", (byte)1);
        keyType.put("page", (byte)2);
        keyType.put("uid", (byte)3);
        keyType.put("did", (byte)4);
    }

    public static final int INDEX_KEY_SIZE = 4;
    public static void ensure_dir(String dir) {
        if (isEmptyStr(dir))
            throw new RuntimeException("null log dir");

        File dirFile = new File(dir);
        if (!dirFile.exists()) {
            try{
                if (dirFile.mkdir()) {
                    logger.info("successfully created new log dir: " + dir);
                }
            }
            catch(SecurityException se){
                throw new RuntimeException("failed to create new log dir: " + dir, se);
            }
        } else {
            logger.info("use the existing log dir: " + dir);
        }

    }

    /**
     * This is to get the index key
     * First byte is key_type ie: ip, user, did, page?
     * When writing to leveldb, we will in fact use the first + next_three. We have a bug before.
     * <p/>
     * Bug example: ip1 and ip2 have the same ipc, but different last segment , this method, {@code getIndexKeyBytes}
     * will generate to two different bytes arrays, when cached before write to leveldb, there will be 2 entries.
     * Then we will write to leveldb, and the index key will actually use first 4 bytes, problem happens,
     * ip2's index key will override ip1's if they are in a same 10-seconds.
     * @param headerField
     * @param key
     * @return
     * @throws UnknownHostException
     */
    public static byte[] getIndexKeyBytes(String headerField, String key) throws UnknownHostException {
        try {
            byte[] bKey;
            if (key == null) {
                key = "";
            }
            if (headerField.equals("c_ip")) {
                bKey = InetAddress.getByName(key).getAddress();
            } else {
                Integer hash = Hashing.murmur3_32().hashString(key, Charset.defaultCharset()).asInt();
                if (hash < 0) {
                    hash *= -1;
                }
                bKey = Ints.toByteArray(hash);

                ArrayUtils.reverse(bKey);
            }

            byte type = EventPersistCommon.getKeyType(headerField);
            // we must strict the size to actual used size, or will occur overriding.
            byte[] result = new byte[INDEX_KEY_SIZE];
            result[0] = type;
            int writeSize = INDEX_KEY_SIZE <= bKey.length ? INDEX_KEY_SIZE : bKey.length;
            System.arraycopy(bKey, 0, result, 1, writeSize - 1);

            return result;
        } catch (Exception e) {
            throw new RuntimeException(String.format("headerField: %s, key: %s", headerField, key), e);
        }
    }

    public static Byte getKeyType(String keyName) {
        return keyType.get(keyName);
    }
}

package com.threathunter.persistent.core.util;

import com.threathunter.persistent.core.io.BufferedRandomAccessFile;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 */
public class BRAFileHelper {

    private static final Map<String, Decoder> DECODER_MAP;

    static {
        DECODER_MAP = new HashMap<>();

        DECODER_MAP.put("int", (file, len) -> getNextFieldInteger(file, len));
        DECODER_MAP.put("long", (file, len) -> getNextFieldLong(file, len));
        DECODER_MAP.put("bool", (file, len) -> getNextFieldBoolean(file, len));
        DECODER_MAP.put("string", (file, len) -> getNextFieldString(file, len));
        DECODER_MAP.put("double", (file, len) -> getNextFieldDouble(file, len));
    }

    public static Integer getNextFieldSize(BufferedRandomAccessFile file, int size) {
        if (size == 0) {
            return 0;
        }
        byte[] sb = new byte[4];
        sb[0] = 0;
        sb[1] = 0;
        byte[] b = getFieldBytes(file, 2);
        sb[2] = b[0];
        sb[3] = b[1];

        return Ints.fromByteArray(sb);
    }

    public static byte[] getFieldBytes(BufferedRandomAccessFile file, int length) {
        byte[] b = new byte[length];

        try {
            int offset = 0;
            while (offset < length) {
                int retSize = file.read(b, offset, length - offset);
                if (retSize < 0) {
                    break;
                }

                offset += retSize;
            }
        } catch (IOException e) {
            return new byte[0];
        }

        return b;
    }

    public static boolean checkCodeValid(BufferedRandomAccessFile file, long currentOffset,
                                         int eventSize) throws IOException {
        int checkCodeSize = 8 - (eventSize % 8);
        file.seek(currentOffset + eventSize);

        byte[] checkBytes = getFieldBytes(file, checkCodeSize);
        for (byte b : checkBytes) {
            if (b != ConstantsUtil.CHECK_CODE) {
                return false;
            }
        }
        return true;
    }

    public static Long getNextFieldLong(BufferedRandomAccessFile file, int size) {
        if (size == 0) {
            return null;
        }
        byte[] b = getFieldBytes(file, 8);
        return BytesDecoder.getLongFromBytes64(b);
    }

    public static String getNextFieldString(BufferedRandomAccessFile file, int length) {
        byte[] b = getFieldBytes(file, length);
        return BytesDecoder.getStringFromBytesString(b);
    }

    public static Object parseField(BufferedRandomAccessFile file, String fieldType, int size) {
        if (!DECODER_MAP.containsKey(fieldType)) {
            return null;
        }
        return DECODER_MAP.get(fieldType).decodeField(file, size);
    }

    public static Integer getNextFieldInteger(BufferedRandomAccessFile file, int size) {
        if (size == 0) {
            return null;
        }
        byte[] b = getFieldBytes(file, 8);
        return BytesDecoder.getIntFromBytes64(b);
    }

    public static Boolean getNextFieldBoolean(BufferedRandomAccessFile file, int size) {
        if (size == 0) {
            return null;
        }
        byte[] b = getFieldBytes(file, 1);
        return b[0] > 0;
    }

    public static Double getNextFieldDouble(BufferedRandomAccessFile file, int size) {
        if (size == 0) {
            return null;
        }
        byte[] b = getFieldBytes(file, 8);
        return BytesDecoder.getDoubleFromBytes64(b);
    }

    private interface Decoder {

        Object decodeField(BufferedRandomAccessFile file, int size);
    }
}

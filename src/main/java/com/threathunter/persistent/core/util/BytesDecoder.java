package com.threathunter.persistent.core.util;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.io.UnsupportedEncodingException;

/**
 * 
 */
public class BytesDecoder {

    public static String getStringFromBytesString(byte[] b) {
        try {
            return new String(b, "utf-8");
        } catch (UnsupportedEncodingException e) {
            return "";
        }
    }

    public static Integer getIntFromBytes64(byte[] b) {
        byte[] bi = new byte[4];
        bi[0] = b[4];
        bi[1] = b[5];
        bi[2] = b[6];
        bi[3] = b[7];

        return Ints.fromByteArray(bi);
    }

    public static Long getLongFromBytes64(byte[] b) {
        return Longs.fromByteArray(b);
    }

    public static Double getDoubleFromBytes64(byte[] b){
        return BytesEncoderDecoder.decode64_double(b);
    }
}

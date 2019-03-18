package com.threathunter.persistent.core.util;

import java.nio.charset.Charset;

/**
 * 
 */
public class BytesEncoderDecoder {
    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    public static byte[] encodeObj(final Object obj) {
        if (obj == null) {
            return new byte[0];
        }

        // int -> 8 byte big endian, double -> 8 byte big endian, string -> utf-8 encoding, boolean -> 1 byte
        if (obj instanceof Number) {
            if (obj instanceof Float || obj instanceof Double) {
                Double d = ((Number)obj).doubleValue();
                return encode64(Double.doubleToRawLongBits(d));
            } else {
                Long l = ((Number)obj).longValue();
                return encode64(l);
            }
        } else if (obj instanceof String) {
            return ((String)obj).getBytes(DEFAULT_CHARSET);
        } else if (obj instanceof Boolean) {
            return encode8((Boolean)obj);
        } else {
            throw new RuntimeException("unsupported types");
        }
    }

    public static byte[] encode8(final boolean value) {
        byte[] b = new byte[1];
        if (value) {
            b[0] = 1;
        } else {
            b[0] = 0;
        }

        return b;
    }

    public static byte[] encode8(int value) {
        byte[] b = new byte[1];
        b[0] = (byte)value;
        return b;
    }

    public static byte[] encode16(final long value) {
        byte[] b = new byte[2];
        b[0] = (byte)(value >>> 8);
        b[1] = (byte)(value);
        return b;
    }

    public static byte[] encode16(final int value) {
        byte[] b = new byte[2];
        b[0] = (byte)(value >>> 8);
        b[1] = (byte)(value);
        return b;
    }

    public static byte[] encode16(final short value) {
        byte[] b = new byte[2];
        b[0] = (byte)(value >>> 8);
        b[1] = (byte)(value);
        return b;
    }

    public static short decode16_short(byte[] bytes) {
        if (bytes == null || bytes.length != 2) {
            throw new RuntimeException("invalid long bytes from 16 bit data");
        }
        return (short)((bytes[0] & 255) << 8 | (bytes[1] & 255));
    }

    public static int decode16_int(byte[] bytes) {
        if (bytes == null || bytes.length != 2) {
            throw new RuntimeException("invalid long bytes from 16 bit data");
        }
        return ((bytes[0] & 255 << 8) | (bytes[1] & 255));
    }

    public static long decode16_long(byte[] bytes) {
        if (bytes == null || bytes.length != 2) {
            throw new RuntimeException("invalid long bytes from 16 bit data");
        }

        return (long)((bytes[0] & 255) << 8 | (bytes[1] & 255));
    }

    public static byte[] encode32(final long value) {
        byte[] b = new byte[4];
        b[0] = (byte)(value >>> 24);
        b[1] = (byte)(value >>> 16);
        b[2] = (byte)(value >>> 8);
        b[3] = (byte)(value);
        return b;
    }

    public static byte[] encode32(final int value) {
        byte[] b = new byte[4];
        b[0] = (byte)(value >>> 24);
        b[1] = (byte)(value >>> 16);
        b[2] = (byte)(value >>> 8);
        b[3] = (byte)(value);
        return b;
    }

    public static boolean decode8_bool(byte[] bytes) {
        if (bytes == null || bytes.length != 1) {
            throw new RuntimeException("invalid boolean bytes from 8 bit data");
        }

        return bytes[0] != 0;
    }

    public static long decode32_long(byte[] bytes) {
        if (bytes == null || bytes.length != 4) {
            throw new RuntimeException("invalid long bytes from 32 bit data");
        }

        return (long)((bytes[0] & 255) << 24 | (bytes[1] & 255) << 16 | (bytes[2] & 255) << 8 | (bytes[3] & 255));
    }

    public static int decode32_int(byte[] bytes) {
        if (bytes == null || bytes.length != 4) {
            throw new RuntimeException("invalid long bytes from 32 bit data");
        }

        return ((bytes[0] & 255) << 24 | (bytes[1] & 255) << 16 | (bytes[2] & 255) << 8 | (bytes[3] & 255));
    }

    public static byte[] encode64(final long value) {
        byte[] b = new byte[8];
        b[0] = (byte)(value >>> 56);
        b[1] = (byte)(value >>> 48);
        b[2] = (byte)(value >>> 40);
        b[3] = (byte)(value >>> 32);
        b[4] = (byte)(value >>> 24);
        b[5] = (byte)(value >>> 16);
        b[6] = (byte)(value >>> 8);
        b[7] = (byte)value;
        return b;
    }

    public static byte[] encode64(final double value) {
        return encode64(Double.doubleToLongBits(value));
    }

    public static double decode64_double(byte[] bytes) {
        return Double.longBitsToDouble(decode64_long(bytes));
    }

    public static long decode64_long(byte[] bytes) {
        long result = 0;
        if (bytes == null || bytes.length != 8) {
            throw new RuntimeException("invalid long bytes from 64 bit data");
        }

        result = ((long) bytes[0] & 255) << 56 |
                ((long) bytes[1] & 255) << 48 |
                ((long) bytes[2] & 255) << 40 |
                ((long) bytes[3] & 255) << 32 |
                ((long) bytes[4] & 255) << 24 |
                ((long) bytes[5] & 255) << 16 |
                ((long) bytes[6] & 255) << 8 |
                ((long) bytes[7] & 255);

        return result;
    }
}

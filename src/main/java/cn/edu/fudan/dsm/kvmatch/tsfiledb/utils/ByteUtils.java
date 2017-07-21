package cn.edu.fudan.dsm.kvmatch.tsfiledb.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * This is the class implementing conversion of objects and byte arrays.
 *
 * @author Ningting Pan
 */
public class ByteUtils {

    public static byte[] listLongToByteArray(List<Long> offsets) {
        byte[] bytes = new byte[Bytes.SIZEOF_LONG * offsets.size()];
        int curOffset = 0;
        for (Long offset : offsets) {
            System.arraycopy(Bytes.toBytes(offset), 0, bytes, curOffset, Bytes.SIZEOF_LONG);
            curOffset += Bytes.SIZEOF_LONG;
        }
        return bytes;
    }

    public static List<Long> byteArrayToListLong(byte[] bytes) {
        List<Long> offsets = new ArrayList<>();
        int size = bytes.length / Bytes.SIZEOF_LONG;
        int curOffset = 0;
        for (int i=0; i<size; i++) {
            byte[] tmp = new byte[Bytes.SIZEOF_LONG];
            System.arraycopy(bytes, curOffset, tmp, 0, Bytes.SIZEOF_LONG);
            offsets.add(Bytes.toLong(tmp));
            curOffset += Bytes.SIZEOF_LONG;
        }
        return offsets;
    }

    public static byte[] combineTwoByteArrays(byte[] firstBytes, byte[] secondBytes) {
        byte[] combineBytes = new byte[firstBytes.length + secondBytes.length];
        System.arraycopy(firstBytes, 0, combineBytes, 0, firstBytes.length);
        System.arraycopy(secondBytes, 0, combineBytes, firstBytes.length, secondBytes.length);
        return combineBytes;
    }
}

package cn.edu.fudan.dsm.kvmatch.tsfiledb.utils;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.entity.IndexNode;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * This is the class implementing conversion of objects and byte arrays.
 *
 * @author Ningting Pan
 */
public class ByteUtils {

    public static byte[] doubleToByteArray(double value) {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putDouble(value);
        return bytes;
    }

    public static double byteArrayToDouble(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getDouble();
    }

    public static byte[] listIndexNodeToByteArray(List<IndexNode> indexNodes) {
        return null;
    }
}

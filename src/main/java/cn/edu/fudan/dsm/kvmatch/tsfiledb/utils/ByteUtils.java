package cn.edu.fudan.dsm.kvmatch.tsfiledb.utils;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.entity.IndexNode;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * This is the class implementing conversion of objects and byte arrays.
 *
 * @author Ningting Pan
 */
public class ByteUtils {

    public static byte[] doubleToByteArray(double value) {
        byte[] bytes = new byte[Bytes.SIZEOF_DOUBLE];
        ByteBuffer.wrap(bytes).putDouble(value);
        return bytes;
    }

    public static double byteArrayToDouble(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getDouble();
    }

    public static byte[] listIndexNodeToByteArray(List<IndexNode> indexNodes) {
        return null;
    }

    public static byte[] listLongToByteArray(List<Long> offsets) {
        return null;
    }

    public static byte[] combineTwoByteArrays(byte[] firstBytes, byte[] secondBytes) {
        byte[] combineBytes = new byte[firstBytes.length + secondBytes.length];
        System.arraycopy(firstBytes, 0, combineBytes, 0, firstBytes.length);
        System.arraycopy(secondBytes, 0, combineBytes, firstBytes.length, secondBytes.length);
        return combineBytes;
    }
}

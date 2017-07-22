package cn.edu.fudan.dsm.kvmatch.tsfiledb.io;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexNode;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.ByteUtils;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.Bytes;
import cn.edu.thu.tsfile.common.utils.Pair;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Index file reader for KV-match file version.
 *
 * @author Ningting Pan
 */
public class IndexFileReader implements Closeable {

    private File file;

    private RandomAccessFile reader;

    private List<Long> offsets = new ArrayList<>();  // Once open the file, read offsets

    public IndexFileReader(String indexFilePath) throws IOException {
        this.file = new File(indexFilePath);
        this.reader = new RandomAccessFile(file, "r");
        // read offsets' info
        readOffsetInfo();
    }

    private void readOffsetInfo() throws IOException {
        // read from the end of file, get (the position of offsets start)
        byte[] bytes = seekAndRead(file.length() - Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
        long lastLineOffset = Bytes.toLong(bytes);
        long lastLineLength = file.length() - lastLineOffset;
        // get last line of all offsets
        bytes = seekAndRead(lastLineOffset, (int) lastLineLength);
        offsets = ByteUtils.byteArrayToListLong(bytes);
    }

    public Map<Double, IndexNode> readIndexes(double keyFrom, double keyTo) throws IOException {
        Map<Double, IndexNode> indexes = new HashMap<>();  // sort increasingly by key
        int startOffsetId = lowerBound(keyFrom);  // find the first key >= keyFrom
        int endOffsetId = upperBound(keyTo);  // find the last key <= keyTo
        if (startOffsetId != -1 && endOffsetId != -1) {
            for (int i = startOffsetId; i <= endOffsetId; i++) {
                long lengthOfLine = offsets.get(i + 1) - offsets.get(i);
                byte[] bytes = seekAndRead(offsets.get(i), (int) lengthOfLine);
                double key = Bytes.toDouble(bytes, 0);
                byte[] valueBytes = new byte[bytes.length - Bytes.SIZEOF_DOUBLE];
                System.arraycopy(bytes, Bytes.SIZEOF_DOUBLE, valueBytes, 0, valueBytes.length);
                IndexNode value = new IndexNode();
                value.parseBytesCompact(valueBytes);
                indexes.put(key, value);
            }
        }
        return indexes;
    }

    public List<Pair<Double, Pair<Integer, Integer>>> readStatisticInfo() throws IOException {
        long statisticInfoOffset = offsets.get(offsets.size() - 2);
        long offsetInfoOffset = offsets.get(offsets.size() - 1);
        byte[] bytes = seekAndRead(statisticInfoOffset, (int) (offsetInfoOffset - statisticInfoOffset));
        return ByteUtils.byteArrayToListTriple(bytes);
    }

    // left range, return index of List<Long> offsets
    private int lowerBound(double keyFrom) throws IOException {
        int left = 0, right = offsets.size() - 3;
        while (left <= right) {
            int mid = left + ((right - left) >> 1);
            if (getKeyByOffset(offsets.get(mid)) >= keyFrom) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        if (left <= offsets.size() - 3) return left;
        return -1;
    }

    // right range, return index of List<Long> offsets
    private int upperBound(double keyTo) throws IOException {
        int left = 0, right = offsets.size() - 3;
        while (left <= right) {
            int mid = left + ((right - left) >> 1);
            if (getKeyByOffset(offsets.get(mid)) <= keyTo) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        if (right >= 0) return right;
        return -1;
    }

    private double getKeyByOffset(Long offset) throws IOException {
        byte[] bytes = seekAndRead(offset, Bytes.SIZEOF_DOUBLE);
        return Bytes.toDouble(bytes);
    }

    private byte[] seekAndRead(long pos, int lengthOfBytes) throws IOException {
        byte[] bytes = new byte[lengthOfBytes];
        reader.seek(pos);
        reader.read(bytes, 0, lengthOfBytes);
        return bytes;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}

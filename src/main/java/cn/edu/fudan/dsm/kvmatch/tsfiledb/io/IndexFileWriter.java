package cn.edu.fudan.dsm.kvmatch.tsfiledb.io;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexNode;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.ByteUtils;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.Bytes;
import cn.edu.thu.tsfile.common.utils.Pair;

import java.io.*;
import java.util.*;

/**
 * Index file writer for KV-match file version.
 *
 * @author Ningting Pan
 */
public class IndexFileWriter implements Closeable {

    private BufferedOutputStream writer;

    private long offset;

    private List<Long> offsets = new ArrayList<>();  // last line

    public IndexFileWriter(String targetFilePath) throws IOException {
        File file = new File(targetFilePath);
        if (file.getParentFile() != null && !file.getParentFile().exists()) {
            if (!file.getParentFile().mkdirs()) {
                throw new IOException("Can not create directory " + file.getParent());
            }
        }
        this.writer = new BufferedOutputStream(new FileOutputStream(file));
        offset = 0L;
    }

    public void writeIndexes(Map<Double, IndexNode> indexes) throws IOException {
        // sort index map by key
        TreeMap<Double, IndexNode> sortedIndexes = new TreeMap<>(indexes);
        for (Map.Entry<Double, IndexNode> entry : sortedIndexes.entrySet()) {
            writeIndex(entry.getKey(), entry.getValue());
        }
    }

    public void writeIndex(double key, IndexNode value) throws IOException {
        offsets.add(offset);
        byte[] keyBytes = Bytes.toBytes(key);
        byte[] valueBytes = value.toBytesCompact();
        byte[] oneLineBytes = ByteUtils.combineTwoByteArrays(keyBytes, valueBytes);
        // write result and record offset
        writeBytesToFile(oneLineBytes);
        offset += keyBytes.length + valueBytes.length;
    }

    public void writeStatisticInfo(List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) throws IOException {
        offsets.add(offset);
        // store statistic information for query order optimization
        statisticInfo.sort(Comparator.comparingDouble(o -> o.left));
        byte[] result = ByteUtils.listTripleToByteArray(statisticInfo);
        // write result and record offset
        writeBytesToFile(result);
        offset += result.length;
    }

    @Override
    public void close() throws IOException {
        // write file offset information to the end of file
        writeOffsetInfo();
        writer.close();
    }

    private void writeOffsetInfo() throws IOException {
        offsets.add(offset);
        writeBytesToFile(ByteUtils.listLongToByteArray(offsets));
    }

    private void writeBytesToFile(byte[] bytes) throws IOException {
        writer.write(bytes, 0, bytes.length);
        writer.flush();
    }
}

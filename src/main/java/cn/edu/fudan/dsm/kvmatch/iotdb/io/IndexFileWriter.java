package cn.edu.fudan.dsm.kvmatch.iotdb.io;

import cn.edu.fudan.dsm.kvmatch.iotdb.common.IndexNode;
import cn.edu.fudan.dsm.kvmatch.iotdb.utils.ByteUtils;
import cn.edu.fudan.dsm.kvmatch.iotdb.utils.Bytes;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Index file writer for KV-match file version.
 *
 * @author Ningting Pan
 */
public class IndexFileWriter implements Closeable {

    private static final String BUILDING_SUFFIX = ".building";

    private String targetFilePath;

    private BufferedOutputStream writer;

    private long offset = 0L;

    private List<Long> offsets = new ArrayList<>();  // last line

    public IndexFileWriter(String targetFilePath) throws IOException {
        this.targetFilePath = targetFilePath;
        File file = new File(targetFilePath + ".building");
        FileUtils.forceMkdirParent(file);
        this.writer = new BufferedOutputStream(new FileOutputStream(file));
    }

    public void write(Map<Double, IndexNode> sortedIndexes, List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) throws IOException {
        offset = 0L;
        offsets.clear();
        writeIndexes(sortedIndexes);
        writeStatisticInfo(statisticInfo);
        writeOffsetInfo();  // write file offset information to the end of file
    }

    private void writeIndexes(Map<Double, IndexNode> sortedIndexes) throws IOException {
        for (Map.Entry<Double, IndexNode> entry : sortedIndexes.entrySet()) {
            writeIndex(entry.getKey(), entry.getValue());
        }
    }

    private void writeIndex(double key, IndexNode value) throws IOException {
        offsets.add(offset);
        byte[] keyBytes = Bytes.toBytes(key);
        byte[] valueBytes = value.toBytesCompact();
        byte[] oneLineBytes = ByteUtils.combineTwoByteArrays(keyBytes, valueBytes);
        // write result and record offset
        writeBytesToFile(oneLineBytes);
        offset += keyBytes.length + valueBytes.length;
    }

    private void writeStatisticInfo(List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) throws IOException {
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
        writer.close();
        // rename the building file to desired name, delete the old obsolete file at first
        File targetFile = new File(targetFilePath);
        FileUtils.deleteQuietly(targetFile);
        File tmpFile = new File(targetFilePath + BUILDING_SUFFIX);
        if (!tmpFile.renameTo(targetFile)) {
            throw new IOException("Failed to rename index file '" + tmpFile + "' to '" + targetFile + "'");
        }
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

package cn.edu.fudan.dsm.kvmatch.tsfiledb.io;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexNode;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.Pair;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.ByteUtils;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.Bytes;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.MeanIntervalUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Index file writer for KV-match file version
 *
 * @author Ningting Pan
 */
public class IndexFileWriter implements Closeable {

    private File file;
    private BufferedOutputStream writer;
    long offset;
    List<Long> offsets = new ArrayList<>(); //last line

    public IndexFileWriter(String targetFilePath) throws FileNotFoundException {
        this.file = new File(targetFilePath);
        this.writer = new BufferedOutputStream(new FileOutputStream(file));
    }

    public void writeIndexes(Map<Double, List<IndexNode>> indexes) {
        long startOffset = 0;
        offset = startOffset;
        for (Map.Entry<Double, List<IndexNode>> entry : indexes.entrySet()) {
            offsets.add(offset);
            double key = entry.getKey();
            List<IndexNode> indexNodes = entry.getValue();
            byte[] keyBytes = ByteUtils.doubleToByteArray(key);
            byte[] indexNodesBytes = ByteUtils.listIndexNodeToByteArray(indexNodes);
            byte[] oneLineBytes = ByteUtils.combineTwoByteArrays(keyBytes, indexNodesBytes);
            // write result and record offset
            writeALineBytesToFile(oneLineBytes);
            offset += keyBytes.length + indexNodesBytes.length;
        }
    }

    public void writeIndex(double key, IndexNode value) {

    }

    public void writeStatisticInfo(List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) {
        offsets.add(offset);
        // store statistic information for query order optimization
        statisticInfo.sort(Comparator.comparing(Pair::getFirst));
        byte[] result = new byte[(Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_INT) * statisticInfo.size()];
        System.arraycopy(MeanIntervalUtils.toBytes(statisticInfo.get(0).getFirst()), 0, result, 0, Bytes.SIZEOF_DOUBLE);
        System.arraycopy(Bytes.toBytes(statisticInfo.get(0).getSecond().getFirst()), 0, result, Bytes.SIZEOF_DOUBLE, Bytes.SIZEOF_INT);
        System.arraycopy(Bytes.toBytes(statisticInfo.get(0).getSecond().getSecond()), 0, result, Bytes.SIZEOF_DOUBLE + Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
        for (int i = 1; i < statisticInfo.size(); i++) {
            statisticInfo.get(i).getSecond().setFirst(statisticInfo.get(i).getSecond().getFirst() + statisticInfo.get(i - 1).getSecond().getFirst());
            statisticInfo.get(i).getSecond().setSecond(statisticInfo.get(i).getSecond().getSecond() + statisticInfo.get(i - 1).getSecond().getSecond());
            System.arraycopy(MeanIntervalUtils.toBytes(statisticInfo.get(i).getFirst()), 0, result, i * (Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_INT), Bytes.SIZEOF_DOUBLE);
            System.arraycopy(Bytes.toBytes(statisticInfo.get(i).getSecond().getFirst()), 0, result, i * (Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_INT) + Bytes.SIZEOF_DOUBLE, Bytes.SIZEOF_INT);
            System.arraycopy(Bytes.toBytes(statisticInfo.get(i).getSecond().getSecond()), 0, result, i * (Bytes.SIZEOF_DOUBLE + 2 * Bytes.SIZEOF_INT) + Bytes.SIZEOF_DOUBLE + Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
        }
        // write result and record offset
        writeALineBytesToFile(result);
        offset += result.length;
        // write file offset information to the end of file
        writeOffsetInfo();
    }

    private void writeOffsetInfo() {
        offsets.add(offset);
        byte[] offsetBytes = ByteUtils.listLongToByteArray(offsets);
        writeALineBytesToFile(offsetBytes);
    }

    @Override
    public void close() throws IOException {
        // write last file index
    }

    private void writeALineBytesToFile(byte[] bytes) {
        try {
            BufferedOutputStream writer = new BufferedOutputStream(new FileOutputStream(file, true));
            writer.write(bytes, 0, bytes.length);
            writer.flush();
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

package cn.edu.fudan.dsm.kvmatch.tsfiledb.io;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexNode;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.Pair;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.ByteUtils;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.Bytes;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.MeanIntervalUtils;

import java.io.*;
import java.util.*;

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
        this.writer = new BufferedOutputStream(new FileOutputStream(file, true));
        offset = 0;
    }

    public void writeIndexes(Map<Double, IndexNode> indexes) {
        // sort index map by key
        TreeMap<Double, IndexNode> sortedIndexes = new TreeMap<>(indexes);
        for (Map.Entry<Double, IndexNode> entry : sortedIndexes.entrySet()) {
            writeIndex(entry.getKey(), entry.getValue());
        }
    }

    public void writeIndex(double key, IndexNode value) {
        offsets.add(offset);
        byte[] keyBytes = Bytes.toBytes(key);
        byte[] valueBytes = value.toBytesCompact();
        byte[] oneLineBytes = ByteUtils.combineTwoByteArrays(keyBytes, valueBytes);
        // write result and record offset
        writeALineBytesToFile(oneLineBytes);
        offset += keyBytes.length + valueBytes.length;
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
    }

    private void writeOffsetInfo() {
        offsets.add(offset);
        byte[] offsetBytes = ByteUtils.listLongToByteArray(offsets);
        writeALineBytesToFile(offsetBytes);
    }

    @Override
    public void close() throws IOException {
        // write file offset information to the end of file
        writeOffsetInfo();
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

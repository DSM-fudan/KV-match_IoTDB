package cn.edu.fudan.dsm.kvmatch.tsfiledb.io;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexNode;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.Pair;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.ByteUtils;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.Bytes;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.MeanIntervalUtils;

import java.io.*;
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

    public IndexFileWriter(String targetFilePath) throws FileNotFoundException {
        this.file = new File(targetFilePath);
        this.writer = new BufferedOutputStream(new FileOutputStream(file));
    }

    public void writeIndexes(Map<Double, List<IndexNode>> indexes) {
        int startOffset = 0;
        for (Map.Entry<Double, List<IndexNode>> entry : indexes.entrySet()) {
            Double key = entry.getKey();
            List<IndexNode> indexNodes = entry.getValue();
            byte[] keyBytes = ByteUtils.doubleToByteArray(key);
            byte[] indexNodesBytes = ByteUtils.listIndexNodeToByteArray(indexNodes);
            writeALineBytesToFile(keyBytes, indexNodesBytes);
        }
    }

    public void writeIndex(double key, IndexNode value) {

    }

    public void writeStatisticInfo(List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) {
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
    }

    @Override
    public void close() throws IOException {
        // write last file index
    }

    private void writeALineBytesToFile(byte[] keyBytes, byte[] indexNodesBytes) {
        try {
            FileInputStream fis = new FileInputStream(file);
            BufferedOutputStream writer = new BufferedOutputStream(new FileOutputStream(file));

            // maintain index row offset in the process

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}

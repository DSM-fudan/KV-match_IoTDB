package cn.edu.fudan.dsm.kvmatch.tsfiledb.io;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexNode;
import cn.edu.thu.tsfile.common.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Ningting Pan
 */
public class IOTest {

    public static void main(String[] args) throws IOException {
        // test on windows
//        String indexPath = "C:\\Users\\dell\\Desktop\\BDMS\\KV-match-TsFileDB\\KV-match_TsFileDB\\index";
        // test on Linux
        String indexPath = "/home/wujy/Dev/kvmatch-tsfiledb/index.test";

        IndexFileWriter indexFileWriter = new IndexFileWriter(indexPath);

        // fake data
        List<Pair<Double, Pair<Integer, Integer>>> statisticInfo = new ArrayList<>();
        double key = 0.1;
        IndexNode indexNode = generateIndexNode();
        statisticInfo.add(new Pair<>(key, new Pair<>(indexNode.getNumOfOffsets(), indexNode.getNumOfIntervals())));
        indexFileWriter.writeIndex(key, indexNode);
        System.out.println(key + "  " + indexNode.toString());

        key = 0.2;
        indexNode = generateIndexNode();
        statisticInfo.add(new Pair<>(key, new Pair<>(indexNode.getNumOfOffsets(), indexNode.getNumOfIntervals())));
        indexFileWriter.writeIndex(key, indexNode);
        System.out.println(key + "  " + indexNode.toString());

        key = 0.3;
        indexNode = generateIndexNode();
        statisticInfo.add(new Pair<>(key, new Pair<>(indexNode.getNumOfOffsets(), indexNode.getNumOfIntervals())));
        indexFileWriter.writeIndex(key, indexNode);
        System.out.println(key + "  " + indexNode.toString());

        indexFileWriter.writeStatisticInfo(statisticInfo);
        indexFileWriter.close();

        // start to read
        IndexFileReader indexFileReader = new IndexFileReader(indexPath);
        Map<Double, IndexNode> indexNodeMap = indexFileReader.readIndexes(0.1, 0.25);
        List<Pair<Double, Pair<Integer, Integer>>> pairList = indexFileReader.readStatisticInfo();
        for (Map.Entry<Double, IndexNode> entry : indexNodeMap.entrySet()) {
            System.out.println("key: " + entry.getKey() + "  value:" + entry.getValue().toString());
        }
        for (Pair<Double, Pair<Integer, Integer>> pair : pairList) {
            System.out.println(pair.left + ":  " + pair.right.left + ";  " + pair.right.right);
        }
        indexFileReader.close();
    }

    private static IndexNode generateIndexNode() {
        IndexNode node = new IndexNode();
        int randomNum = ThreadLocalRandom.current().nextInt(0, 10 + 1);
        for (int i = 0; i < randomNum; i += 3) {
            node.getPositions().add(new Pair<>(i + 100000000000L, i + 100000000002L));
        }
        randomNum = ThreadLocalRandom.current().nextInt(0, 10 + 1);
        for (int i = 0; i < randomNum; i += 3) {
            node.getPositions().add(new Pair<>(i + 1000000000000000L, i + 1000000000000002L));
        }
        return node;
    }
}

package cn.edu.fudan.dsm.kvmatch.tsfiledb.io;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexNode;
import cn.edu.thu.tsfile.common.utils.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Jiaye Wu
 */
public class IOTest {

    private static final String INDEX_PATH = "tmp" + File.separator + "index.test";

    private static Map<Double, IndexNode> indexes = new HashMap<>();

    private static List<Pair<Double, Pair<Integer, Integer>>> statisticInfo = new ArrayList<>();

    @Before
    public void generateData() {
        double key = 0.1;
        IndexNode indexNode = generateIndexNode();
        indexes.put(key, indexNode);
        statisticInfo.add(new Pair<>(key, new Pair<>(indexNode.getNumOfOffsets(), indexNode.getNumOfIntervals())));

        key = 0.2;
        indexNode = generateIndexNode();
        indexes.put(key, indexNode);
        statisticInfo.add(new Pair<>(key, new Pair<>(indexNode.getNumOfOffsets(), indexNode.getNumOfIntervals())));

        key = 0.3;
        indexNode = generateIndexNode();
        indexes.put(key, indexNode);
        statisticInfo.add(new Pair<>(key, new Pair<>(indexNode.getNumOfOffsets(), indexNode.getNumOfIntervals())));

        key = 0.4;
        indexNode = generateIndexNode();
        indexes.put(key, indexNode);
        statisticInfo.add(new Pair<>(key, new Pair<>(indexNode.getNumOfOffsets(), indexNode.getNumOfIntervals())));
    }

    @Test
    public void test() throws IOException {
        // test write
        IndexFileWriter indexFileWriter = new IndexFileWriter(INDEX_PATH);
        indexFileWriter.writeIndexes(indexes);
        indexFileWriter.writeStatisticInfo(statisticInfo);
        indexFileWriter.close();

        // test read
        IndexFileReader indexFileReader = new IndexFileReader(INDEX_PATH);
        Map<Double, IndexNode> indexNodeMap = indexFileReader.readIndexes(0.5, 0.6);
        Assert.assertEquals(indexNodeMap.size(), 0);
        indexNodeMap = indexFileReader.readIndexes(-0.5, 0.0);
        Assert.assertEquals(indexNodeMap.size(), 0);
        indexNodeMap = indexFileReader.readIndexes(-0.5, 10.0);
        Assert.assertEquals(indexNodeMap.size(), 4);
        indexNodeMap = indexFileReader.readIndexes(-0.5, 0.1);
        Assert.assertEquals(indexNodeMap.size(), 1);
        indexNodeMap = indexFileReader.readIndexes(0.4, 10.0);
        Assert.assertEquals(indexNodeMap.size(), 1);
        indexNodeMap = indexFileReader.readIndexes(0.2, 0.35);
        Assert.assertEquals(indexNodeMap.size(), 2);
        for (Map.Entry<Double, IndexNode> entry : indexNodeMap.entrySet()) {
            Assert.assertEquals(entry.getValue(), indexes.get(entry.getKey()));
        }
        List<Pair<Double, Pair<Integer, Integer>>> pairList = indexFileReader.readStatisticInfo();
        for (int i = 0; i < pairList.size(); i++) {
            Pair<Double, Pair<Integer, Integer>> pair = pairList.get(i);
            Assert.assertEquals(pair, statisticInfo.get(i));
        }
        indexFileReader.close();
    }

    @After
    public void clean() {
        File indexFile = new File(INDEX_PATH);
        if (indexFile.exists()) {
            indexFile.delete();
        }
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

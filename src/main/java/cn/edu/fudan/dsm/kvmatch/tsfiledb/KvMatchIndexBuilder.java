package cn.edu.fudan.dsm.kvmatch.tsfiledb;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexConfig;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexNode;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.io.IndexFileWriter;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.statistic.StatisticInfo;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.IndexNodeUtils;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.MeanIntervalUtils;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.support.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * This is the class actually build the KV-match index for specific data series.
 *
 * @author Jiaye Wu
 */
public class KvMatchIndexBuilder implements Callable<Boolean> {

    private static final Logger logger = LoggerFactory.getLogger(KvMatchIndexBuilder.class);

    private IndexConfig indexConfig;

    private Path columnPath;

    private QueryDataSet dataSet;

    private String targetFilePath;

    public KvMatchIndexBuilder(IndexConfig indexConfig, Path columnPath, QueryDataSet dataSet, String targetFilePath) {
        this.indexConfig = indexConfig;
        this.columnPath = columnPath;
        this.dataSet = dataSet;
        this.targetFilePath = targetFilePath;
    }

    @Override
    public Boolean call() throws Exception {
        logger.info("Building index for {}: {}", columnPath, targetFilePath);
        try (IndexFileWriter indexFileWriter = new IndexFileWriter(targetFilePath)) {
            // Step 1: scan data set and extract window features
            Double lastMeanRound = null;
            IndexNode indexNode = null;
            Map<Double, IndexNode> indexNodeMap = new HashMap<>();
            long lastTime = 0;
            double lastValue = 0;
            double ex = 0;
            while (dataSet.next()) {
                RowRecord row = dataSet.getCurrentRecord();

                long curTime = row.getTime();
                double curValue = Double.parseDouble(row.getFields().get(0).getStringValue());  // TODO: improve for performance
                logger.trace("{}: {}", curTime, curValue);
                if (lastTime == 0) {  // TODO: the first window is not right
                    lastTime = curTime - 1;
                    lastValue = curValue;
                }
                double deltaValue = (curValue - lastValue) / (curTime - lastTime);
                for (long time = lastTime + 1; time <= curTime; time++) {
                    ex += lastValue + deltaValue * (time - lastTime);

                    if (time % indexConfig.getWindowLength() == 0) {  // a new disjoint window of data
                        double mean = ex / indexConfig.getWindowLength();
                        ex = 0;

                        double curMeanRound = MeanIntervalUtils.toRound(mean);
                        logger.debug("key: {}, mean: {}, time: {}", curMeanRound, mean, time);

                        long pos = time / indexConfig.getWindowLength();
                        if (lastMeanRound == null || !lastMeanRound.equals(curMeanRound) || pos - indexNode.getPositions().get(indexNode.getPositions().size() - 1).left == IndexNode.MAXIMUM_DIFF - 1) {
                            // put the last row
                            if (lastMeanRound != null) {
                                indexNodeMap.put(lastMeanRound, indexNode);
                            }
                            // new row
                            logger.debug("new row, key: {}", curMeanRound);
                            indexNode = indexNodeMap.get(curMeanRound);
                            if (indexNode == null) {
                                indexNode = new IndexNode();
                            }
                            indexNode.getPositions().add(new Pair<>(pos, pos));
                            lastMeanRound = curMeanRound;
                        } else {
                            // use last row
                            logger.debug("use last row, key: {}", lastMeanRound);
                            int index = indexNode.getPositions().size();
                            indexNode.getPositions().get(index - 1).right = pos;
                        }
                    }
                }
                lastTime = curTime;
                lastValue = curValue;
            }
            if (indexNode != null && !indexNode.getPositions().isEmpty()) {  // put the last node
                indexNodeMap.put(lastMeanRound, indexNode);
            }

            // Step 2: get ordered statistic list and average number of disjoint window intervals
            List<Pair<Double, Pair<Integer, Integer>>> rawStatisticInfo = new ArrayList<>(indexNodeMap.size());
            StatisticInfo average = new StatisticInfo();
            for (Map.Entry entry : indexNodeMap.entrySet()) {
                IndexNode indexNode1 = (IndexNode) entry.getValue();
                rawStatisticInfo.add(new Pair<>((Double) entry.getKey(), new Pair<>(indexNode1.getPositions().size(), 0)));
                average.append(indexNode1.getPositions().size());
            }
            rawStatisticInfo.sort((o1, o2) -> -o1.left.compareTo(o2.left));
            logger.info("number of disjoint window intervals: average: {}, minimum: {}, maximum: {}", average.getAverage(), average.getMinimum(), average.getMaximum());

            // Step 3: merge adjacent index nodes satisfied criterion, and store to index file
            Map<Double, IndexNode> indexStore = new TreeMap<>();
            List<Pair<Double, Pair<Integer, Integer>>> statisticInfo = new ArrayList<>(indexNodeMap.size());
            IndexNode last = indexNodeMap.get(rawStatisticInfo.get(0).left);
            for (int i = 1; i < rawStatisticInfo.size(); i++) {
                IndexNode current = indexNodeMap.get(rawStatisticInfo.get(i).left);
                boolean isMerged = false;
                if (rawStatisticInfo.get(i).right.left < average.getAverage() * 1.2) {
                    IndexNode merged = IndexNodeUtils.mergeIndexNode(last, current);
                    if (merged.getPositions().size() < (last.getPositions().size() + current.getPositions().size()) * 0.8) {
                        logger.debug("[MERGE] {} - last: {}, current: {}, merged: {}", rawStatisticInfo.get(i - 1).left, last.getPositions().size(), current.getPositions().size(), merged.getPositions().size());
                        last = merged;
                        isMerged = true;
                    }
                }
                if (!isMerged) {
                    double key = rawStatisticInfo.get(i - 1).left;
                    indexStore.put(key, last);
                    statisticInfo.add(new Pair<>(key, last.getStatisticInfoPair()));
                    last = current;
                }
            }
            double key = rawStatisticInfo.get(rawStatisticInfo.size() - 1).left;  // store the last row to index file
            indexStore.put(key, last);
            statisticInfo.add(new Pair<>(key, last.getStatisticInfoPair()));

            // Step 4: store to index file and close file
            indexFileWriter.write(indexStore, statisticInfo);

            return true;
        } catch (IOException e) {
            logger.error(e.getMessage(), e.getCause());
        }
        return false;
    }
}

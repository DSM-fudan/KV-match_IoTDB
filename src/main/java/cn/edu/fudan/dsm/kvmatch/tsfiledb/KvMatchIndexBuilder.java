package cn.edu.fudan.dsm.kvmatch.tsfiledb;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexNode;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.Pair;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.io.IndexFileWriter;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.statistic.StatisticInfo;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.IndexNodeUtils;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.MeanIntervalUtils;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.support.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is the class actually build the KV-match index for specific data series.
 *
 * @author Jiaye Wu
 */
public class KvMatchIndexBuilder {

    private static final Logger logger = LoggerFactory.getLogger(KvMatchIndexBuilder.class);

    private Path columnPath;

    public KvMatchIndexBuilder(Path columnPath) {
        this.columnPath = columnPath;
    }

    public boolean build(QueryDataSet dataSet, String targetFilePath) throws FileNotFoundException {
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
            logger.debug("{}: {}", curTime, curValue);
            if (lastTime == 0) {  // TODO: the first window is not right
                lastTime = curTime - 1;
                lastValue = curValue;
            }
            double deltaValue = (curValue - lastValue) / (curTime - lastTime);
            for (long time = lastTime + 1; time <= curTime; time++) {
                ex += lastValue + deltaValue * (time - lastTime);

                if (time % KvMatchConfig.WINDOW_LENGTH == 0) {  // a new disjoint window of data
                    double mean = ex / KvMatchConfig.WINDOW_LENGTH;
                    ex = 0;

                    double curMeanRound = MeanIntervalUtils.toRound(mean);
                    logger.debug("key: {}, mean: {}, time: {}", curMeanRound, mean, time);

                    long pos = time / KvMatchConfig.WINDOW_LENGTH;
                    if (lastMeanRound == null || !lastMeanRound.equals(curMeanRound) || pos - indexNode.getPositions().get(indexNode.getPositions().size() - 1).getFirst() == IndexNode.MAXIMUM_DIFF - 1) {
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
                        indexNode.getPositions().get(index - 1).setSecond(pos);
                    }
                }
            }
            lastTime = curTime;
            lastValue = curValue;
        }
        if (indexNode != null && !indexNode.getPositions().isEmpty()) {  // put the last node
            indexNodeMap.put(lastMeanRound, indexNode);
        }

        // Step 2: make up index structure
        // get ordered statistic list and average number of disjoint window intervals
        List<Pair<Double, Pair<Integer, Integer>>> rawStatisticInfo = new ArrayList<>(indexNodeMap.size());
        StatisticInfo average = new StatisticInfo();
        for (Map.Entry entry : indexNodeMap.entrySet()) {
            IndexNode indexNode1 = (IndexNode) entry.getValue();
            rawStatisticInfo.add(new Pair<>((Double) entry.getKey(), new Pair<>(indexNode1.getPositions().size(), 0)));
            average.append(indexNode1.getPositions().size());
        }
        rawStatisticInfo.sort((o1, o2) -> -o1.getFirst().compareTo(o2.getFirst()));
        logger.info("number of disjoint window intervals: average: {}, minimum: {}, maximum: {}", average.getAverage(), average.getMinimum(), average.getMaximum());

        // merge adjacent index nodes satisfied criterion, and store to index file
        IndexFileWriter indexFileWriter = new IndexFileWriter(targetFilePath);
        List<Pair<Double, Pair<Integer, Integer>>> statisticInfo = new ArrayList<>(indexNodeMap.size());
        IndexNode last = indexNodeMap.get(rawStatisticInfo.get(0).getFirst());
        for (int i = 1; i < rawStatisticInfo.size(); i++) {
            IndexNode current = indexNodeMap.get(rawStatisticInfo.get(i).getFirst());
            boolean isMerged = false;
            if (rawStatisticInfo.get(i).getSecond().getFirst() < average.getAverage() * 1.2) {
                IndexNode merged = IndexNodeUtils.mergeIndexNode(last, current);
                if (merged.getPositions().size() < (last.getPositions().size() + current.getPositions().size()) * 0.8) {
                    logger.debug("[MERGE] {} - last: {}, current: {}, merged: {}", rawStatisticInfo.get(i-1).getFirst(), last.getPositions().size(), current.getPositions().size(), merged.getPositions().size());
                    last = merged;
                    isMerged = true;
                }
            }
            if (!isMerged) {
                double key = rawStatisticInfo.get(i-1).getFirst();
                indexFileWriter.writeIndex(key, last);
                statisticInfo.add(new Pair<>(key, last.getStatisticInfoPair()));
                last = current;
            }
        }
        // store the last row to index file
        double key = rawStatisticInfo.get(rawStatisticInfo.size()-1).getFirst();
        indexFileWriter.writeIndex(key, last);
        statisticInfo.add(new Pair<>(key, last.getStatisticInfoPair()));

        // Step 3: store to disk
        indexFileWriter.writeStatisticInfo(statisticInfo);
        return true;
    }
}

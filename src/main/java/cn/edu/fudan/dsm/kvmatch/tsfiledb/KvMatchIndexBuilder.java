package cn.edu.fudan.dsm.kvmatch.tsfiledb;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.entity.IndexNode;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.MeanIntervalUtils;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.support.RowRecord;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
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

    public boolean build(QueryDataSet dataSet, String targetFilePath) {
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
            if (lastTime == 0) {
                lastTime = curTime - 1;
                lastValue = curValue;
            }
            double deltaValue = (curValue - lastValue) / (curTime - lastTime);
            for (long time = lastTime + 1; time <= curTime; time++) {
                logger.debug("+{}: {}", time, lastValue + deltaValue * (time - lastTime));
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
        for (Map.Entry<Double, IndexNode> entry : indexNodeMap.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue().toString());
        }

        // Step 3: store to disk

        return false;
    }
}

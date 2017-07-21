package cn.edu.fudan.dsm.kvmatch.tsfiledb;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.*;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.io.IndexFileReader;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.MeanIntervalUtils;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * This is the class actually execute the KV-match index query processing for one index file.
 *
 * @author Jiaye Wu
 */
public class KvMatchQueryExecutor implements Callable<QueryResult> {

    private static final Logger logger = LoggerFactory.getLogger(KvMatchQueryExecutor.class);

    private QueryConfig queryConfig;

    private Path columnPath;

    private String indexFilePath;

    private List<Pair<Double, Pair<Integer, Integer>>> statisticInfo;

    private List<IndexCache> indexCache;

    private int lenQ, windowLength;
    private double epsilon, alpha, beta, meanQ, stdQ;

    public KvMatchQueryExecutor(QueryConfig queryConfig, Path columnPath, String indexFilePath) {
        this.queryConfig = queryConfig;
        this.columnPath = columnPath;
        this.indexFilePath = indexFilePath;

        epsilon = queryConfig.getEpsilon();
        alpha = queryConfig.getAlpha();
        beta = queryConfig.getBeta();
        lenQ = queryConfig.getQuerySeries().size();
        windowLength = queryConfig.getWindowLength();
    }

    @Override
    public QueryResult call() throws Exception {
        logger.info("Querying index for {}: {}", columnPath, indexFilePath);
        try (IndexFileReader reader = new IndexFileReader(indexFilePath)) {
            statisticInfo = reader.readStatisticInfo();

            List<QuerySegment> queries = determineQueryPlan();
            logger.info("Query order: {}", queries);

            List<Interval> validPositions = new ArrayList<>();
            int lastSegment = queries.get(queries.size() - 1).getOrder();
            double lastTotalTimeUsageEstimated = Double.MAX_VALUE;
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < queries.size(); i++) {
                QuerySegment query = queries.get(i);

                logger.info("Window #{} - {} - meanMin:{} - meanMax:{}", i + 1, query.getOrder(), query.getMeanMin(), query.getMeanMax());

                int deltaW = (i == queries.size() - 1) ? 0 : (queries.get(i + 1).getOrder() - query.getOrder());// * WuList[0];

                List<Interval> nextValidPositions = new ArrayList<>();

                // store possible current segment
                List<Interval> positions = new ArrayList<>();

                // query possible rows which mean is in possible distance range of i th disjoint window
                double beginRound = 1.0/alpha * query.getMeanMin() + (1 - 1.0/alpha) * meanQ - beta - Math.sqrt(1.0/(alpha*alpha) * stdQ*stdQ * epsilon*epsilon / query.getWindowLength());
                double beginRound1 = alpha * query.getMeanMin() + (1 - alpha) * meanQ - beta - Math.sqrt(alpha*alpha * stdQ*stdQ * epsilon*epsilon / query.getWindowLength());
                beginRound = MeanIntervalUtils.toRound(Math.min(beginRound, beginRound1), statisticInfo);

                double endRound = alpha * query.getMeanMax() + (1 - alpha) * meanQ + beta + Math.sqrt(alpha*alpha * stdQ*stdQ * epsilon*epsilon / query.getWindowLength());
                double endRound1 = 1.0/alpha * query.getMeanMax() + (1 - 1.0/alpha) * meanQ + beta + Math.sqrt(1.0/(alpha*alpha) * stdQ*stdQ * epsilon*epsilon / query.getWindowLength());
                endRound = MeanIntervalUtils.toRound(Math.max(endRound, endRound1));

                logger.info("Scan index from {} to {}", beginRound, endRound);
                if (queryConfig.isUseCache()) {
                    int index_l = findCache(beginRound);
                    int index_r = findCache(endRound, index_l);

                    if (index_l == index_r && index_l >= 0) {
                        /*
                         * Current:          l|===|r
                         * Cache  : index_l_l|_____|index_l_r
                         * Future : index_l_l|_____|index_l_r
                         */
                        scanCache(index_l, beginRound, true, endRound, true, query, positions);
                    } else if (index_l < 0 && index_r >= 0) {
                        /*
                         * Current:         l|_==|r
                         * Cache  :   index_r_l|_____|index_r_r
                         * Future : index_r_l|_______|index_r_r
                         */
                        scanCache(index_r, indexCache.get(index_r).getBeginRound(), true, endRound, true, query, positions);
                        scanIndexAndAddCache(reader, beginRound, true, indexCache.get(index_r).getBeginRound(), false, index_r, query, positions);
                        indexCache.get(index_r).setBeginRound(beginRound);
                    } else if (index_l >= 0 && index_r < 0) {
                        /*
                         * Current:             l|==_|r
                         * Cache  : index_l_l|_____|index_l_r
                         * Future : index_l_l|_______|index_l_r
                         */
                        scanCache(index_l, beginRound, true, indexCache.get(index_l).getEndRound(), true, query, positions);
                        scanIndexAndAddCache(reader, indexCache.get(index_l).getEndRound(), false, endRound, true, index_l, query, positions);
                        indexCache.get(index_l).setEndRound(endRound);
                    } else if (index_l == index_r && index_l < 0) {
                        /*
                         * Current:        l|___|r
                         * Cache  : |_____|       |_____|
                         * Future : |_____|l|___|r|_____|
                         */
                        scanIndexAndAddCache(reader, beginRound, true, endRound, true, index_r, query, positions);  // insert a new cache node
                    } else if (index_l >= 0 && index_r >= 0 && index_l + 1 == index_r) {
                        /*
                          Current:     l|=___=|r
                          Cache  : |_____|s  |_____|
                          Future : |_______________|
                         */
                        double s = indexCache.get(index_l).getEndRound();
                        scanCache(index_l, beginRound, true, s, true, query, positions);
                        scanIndexAndAddCache(reader, s, false, indexCache.get(index_r).getBeginRound(), false, index_r, query, positions);
                        scanCache(index_r, indexCache.get(index_r).getBeginRound(), true, endRound, true, query, positions);
                        indexCache.get(index_r).setBeginRound(s + 0.01);
                    }
                } else {
                    scanIndex(reader, beginRound, true, endRound, true, query, positions);
                }
                positions = sortButNotMergeIntervals(positions);

                if (i == 0) {
                    for (Interval position : positions) {
                        nextValidPositions.add(new Interval(position.getLeft() + deltaW, position.getRight() + deltaW, position.getEx(), position.getEx2()));
                    }
                } else {
                    int index1 = 0, index2 = 0;  // 1 - validPositions, 2-positions
                    while (index1 < validPositions.size() && index2 < positions.size()) {
                        if (validPositions.get(index1).getRight() < positions.get(index2).getLeft()) {
                            index1++;
                        } else if (positions.get(index2).getRight() < validPositions.get(index1).getLeft()) {
                            index2++;
                        } else {
                            double sumEx = validPositions.get(index1).getEx() + positions.get(index2).getEx();
                            double sumEx2 = validPositions.get(index1).getEx2() + positions.get(index2).getEx2();
                            double mean = sumEx / (i + 1);  // w_i are identical, so they are omitted to avoid exceeding type limit
                            double newValue;
                            double std2 = 0;
                            if (mean > meanQ + beta) {
                                newValue = meanQ + beta - (mean - meanQ - beta) * (i + 1) * windowLength / (lenQ - (i + 1) * 1.0 * windowLength);
                                mean = meanQ + beta;
                                std2 = (sumEx2 * 1.0 * windowLength + (lenQ - (i + 1) * 1.0 * windowLength) * newValue * newValue) / lenQ - mean * mean;
                            }
                            if (validPositions.get(index1).getRight() < positions.get(index2).getRight()) {
                                if (Double.compare(std2, alpha * alpha * stdQ * stdQ) <= 0) {
                                    nextValidPositions.add(new Interval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, validPositions.get(index1).getRight() + deltaW, sumEx, sumEx2));
                                }
                                index1++;
                            } else {
                                if (Double.compare(std2, alpha * alpha * stdQ * stdQ) <= 0) {
                                    nextValidPositions.add(new Interval(Math.max(validPositions.get(index1).getLeft(), positions.get(index2).getLeft()) + deltaW, positions.get(index2).getRight() + deltaW, sumEx, sumEx2));
                                }
                                index2++;
                            }
                        }
                    }
                }

                Pair<List<Interval>, Pair<Integer, Long>> candidates = sortButNotMergeIntervalsAndCount(nextValidPositions);
                validPositions = candidates.left;
//                logger.info("next valid: {}", validPositions.toString());

                int cntCurrentDisjointCandidateWindows = candidates.right.left;
                long cntCurrentCandidateOffsets = candidates.right.right * windowLength;
                logger.info("Disjoint candidate windows: {}, candidate offsets: {}", cntCurrentDisjointCandidateWindows, cntCurrentCandidateOffsets);

                int step1TimeUsageUntilNow = (int) (System.currentTimeMillis() - startTime);
                double step2TimeUsageEstimated = QueryConfig.STEP_2_TIME_ESTIMATE_COEFFICIENT_A * cntCurrentDisjointCandidateWindows + QueryConfig.STEP_2_TIME_ESTIMATE_COEFFICIENT_B * cntCurrentCandidateOffsets / 100000 * lenQ + QueryConfig.STEP_2_TIME_ESTIMATE_INTERCEPT;
                double totalTimeUsageEstimated = step1TimeUsageUntilNow + step2TimeUsageEstimated;
                logger.info("Time usage: step 1 until now: {}, step 2 estimated: {}, total estimated: {}", step1TimeUsageUntilNow, step2TimeUsageEstimated, totalTimeUsageEstimated);

                if (i >= 5 && totalTimeUsageEstimated > lastTotalTimeUsageEstimated) {
                    lastSegment = (i == queries.size() - 1) ? query.getOrder() : queries.get(i + 1).getOrder();
                    break;
                }
                lastTotalTimeUsageEstimated = totalTimeUsageEstimated;
            }

            // merge consecutive intervals to shrink data size and alleviate scan times
            validPositions = sortAndMergeIntervals(validPositions);

            for (Interval validPosition : validPositions) {
                long begin = (validPosition.getLeft() - (lastSegment - 1) - 1) * windowLength + 1 - windowLength + 1;
                long end = (validPosition.getRight() - (lastSegment - 1) - 1) * windowLength + 1 + lenQ - 1;
                if (begin < 1) begin = 1;
            }
        }

        return new QueryResult();
    }

    private Pair<Integer, Integer> getCountsFromStatisticInfo(int Wu, double meanMin, double meanMax) {
        double beginRound = 1.0/alpha * meanMin + (1 - 1.0/alpha) * meanQ - beta - Math.sqrt(1.0/(alpha*alpha) * stdQ*stdQ * epsilon*epsilon / Wu);
        double beginRound1 = alpha * meanMin + (1 - alpha) * meanQ - beta - Math.sqrt(alpha*alpha * stdQ*stdQ * epsilon*epsilon / Wu);
        beginRound = MeanIntervalUtils.toRound(Math.min(beginRound, beginRound1));

        double endRound = alpha * meanMax + (1 - alpha) * meanQ + beta + Math.sqrt(alpha*alpha * stdQ*stdQ * epsilon*epsilon / Wu);
        double endRound1 = 1.0/alpha * meanMax + (1 - 1.0/alpha) * meanQ + beta + Math.sqrt(1.0/(alpha*alpha) * stdQ*stdQ * epsilon*epsilon / Wu);
        endRound = MeanIntervalUtils.toRound(Math.max(endRound, endRound1));

        int index = Collections.binarySearch(statisticInfo, new Pair<>(beginRound, 0), Comparator.comparing(o -> o.left));
        index = index < 0 ? -(index + 1) : index;
        if (index >= statisticInfo.size()) index = statisticInfo.size() - 1;
        int lower1 = index > 0 ? statisticInfo.get(index - 1).right.left : 0;
        int lower2 = index > 0 ? statisticInfo.get(index - 1).right.right : 0;

        index = Collections.binarySearch(statisticInfo, new Pair<>(endRound, 0), Comparator.comparing(o -> o.left));
        index = index < 0 ? -(index + 1) : index;
        if (index >= statisticInfo.size()) index = statisticInfo.size() - 1;
        int upper1 = index > 0 ? statisticInfo.get(index).right.left : 0;
        int upper2 = index > 0 ? statisticInfo.get(index).right.right : 0;

        return new Pair<>(upper1 - lower1, upper2 - lower2);
    }

    private List<QuerySegment> determineQueryPlan() {
        List<QuerySegment> queries = new ArrayList<>();

        // calculate mean and std of whole query series
        double ex = 0, ex2 = 0;
        for (int i = 0; i < lenQ; i++) {
            double value = queryConfig.getQuerySeries().get(i);
            ex += value;
            ex2 += value * value;
        }
        meanQ = ex / lenQ;
        stdQ = Math.sqrt(ex2 / lenQ - meanQ * meanQ);

        logger.info("meanQ: {}, stdQ: {}, alpha: {}, beta: {}", meanQ, stdQ, alpha, beta);

        // sliding windows
        ex = 0;
        for (int i = 0; i < windowLength - 1; i++) {
            ex += queryConfig.getQuerySeries().get(i);
        }
        for (int i = 0; i + 2 * windowLength - 1 <= lenQ; i += windowLength) {
            double meanMin = 1000000, meanMax = -1000000;
            for (int j = i + windowLength - 1; j < i + 2 * windowLength - 1; j++) {
                ex += queryConfig.getQuerySeries().get(j);
                if (j - windowLength >= 0) {
                    ex -= queryConfig.getQuerySeries().get(j - windowLength);
                }
                double mean = ex / windowLength;
                meanMin = Math.min(meanMin, mean);
                meanMax = Math.max(meanMax, mean);
            }
            queries.add(new QuerySegment(meanMin, meanMax, i / windowLength + 1, getCountsFromStatisticInfo(windowLength, meanMin, meanMax).left, windowLength));
        }

        // optimize query order
        queries.sort(Comparator.comparingInt(QuerySegment::getCount));

        return queries;
    }

    private void scanIndex(IndexFileReader reader, double begin, boolean beginInclusive, double end, boolean endInclusive, QuerySegment query, List<Interval> positions) throws IOException {
        if (!beginInclusive) begin = begin + 0.01;
        if (endInclusive) end = end + 0.01;

        for (Map.Entry<Double, IndexNode> entry : reader.readIndexes(begin, end).entrySet()) {
            double meanRound = entry.getKey();
            double meanRound2 = meanRound < 0 ? MeanIntervalUtils.toUpper(meanRound, statisticInfo) : meanRound;
            for (Pair<Long, Long> position : entry.getValue().getPositions()) {
                positions.add(new Interval(position.left, position.right, meanRound, meanRound2 * meanRound2));
            }
        }
    }

    private void scanIndexAndAddCache(IndexFileReader reader, double begin, boolean beginInclusive, double end, boolean endInclusive, int index, QuerySegment query, List<Interval> positions) throws IOException {
        if (index < 0) {
            index = -index - 1;
            indexCache.add(index, new IndexCache(begin, end));
        }

        if (!beginInclusive) begin = begin + 0.01;
        if (endInclusive) end = end + 0.01;

        for (Map.Entry<Double, IndexNode> entry : reader.readIndexes(begin, end).entrySet()) {
            double meanRound = entry.getKey();
            double meanRound2 = meanRound < 0 ? MeanIntervalUtils.toUpper(meanRound, statisticInfo) : meanRound;
            for (Pair<Long, Long> position : entry.getValue().getPositions()) {
                positions.add(new Interval(position.left, position.right, meanRound, meanRound2 * meanRound2));
            }
            indexCache.get(index).addCache(meanRound, entry.getValue());
        }
    }

    private void scanCache(int index, double begin, boolean beginInclusive, double end, boolean endInclusive, QuerySegment query, List<Interval> positions) {
        for (Map.Entry<Double, IndexNode> entry : indexCache.get(index).getCaches().subMap(begin, beginInclusive, end, endInclusive).entrySet()) {
            double meanRound = entry.getKey();
            IndexNode indexNode = entry.getValue();
            double meanRound2 = meanRound < 0 ? MeanIntervalUtils.toUpper(meanRound, statisticInfo) : meanRound;
            for (Pair<Long, Long> position : indexNode.getPositions()) {
                positions.add(new Interval(position.left, position.right, meanRound, meanRound2 * meanRound2));
            }
        }
    }

    private int findCache(double round) {
        return findCache(round, 0);
    }

    private int findCache(double round, int first) {
        if (first < 0) {
            first = -first - 1;
        }

        for (int i = first; i < indexCache.size(); i++) {
            IndexCache cache = indexCache.get(i);
            if (cache.getBeginRound() > round) {
                return -i - 1;
            }
            if (cache.getBeginRound() <= round && cache.getEndRound() >= round) {
                return i;
            }
        }

        return -1;
    }

    private List<Interval> sortButNotMergeIntervals(List<Interval> intervals) {
        if (intervals.size() <= 1) {
            return intervals;
        }

        intervals.sort(Comparator.comparingLong(Interval::getLeft));

        Interval first = intervals.get(0);
        long start = first.getLeft();
        long end = first.getRight();
        double ex = first.getEx();
        double ex2 = first.getEx2();

        List<Interval> result = new ArrayList<>();

        for (int i = 1; i < intervals.size(); i++) {
            Interval current = intervals.get(i);
            if (current.getLeft() - 1 < end || (current.getLeft() - 1 == end && Double.compare(current.getEx(), ex) == 0 && Double.compare(current.getEx2(), ex2) == 0)) {
                end = Math.max(current.getRight(), end);
                ex = Math.min(current.getEx(), ex);
                ex2 = Math.min(current.getEx2(), ex2);
            } else {
                result.add(new Interval(start, end, ex, ex2));
                start = current.getLeft();
                end = current.getRight();
                ex = current.getEx();
                ex2 = current.getEx2();
            }
        }
        result.add(new Interval(start, end, ex, ex2));

        return result;
    }

    private Pair<List<Interval>, Pair<Integer, Long>> sortButNotMergeIntervalsAndCount(List<Interval> intervals) {
        if (intervals.size() <= 1) {
            return new Pair<>(intervals, new Pair<>(intervals.size(), intervals.isEmpty() ? 0 : (intervals.get(0).getRight() - intervals.get(0).getLeft() + 1)));
        }

        intervals.sort(Comparator.comparingLong(Interval::getLeft));

        Interval first = intervals.get(0);
        long start = first.getLeft();
        long end = first.getRight();
        double ex = first.getEx();
        double ex2 = first.getEx2();

        List<Interval> result = new ArrayList<>();

        int cntDisjointIntervals = intervals.size();
        long cntOffsets = 0;
        for (int i = 1; i < intervals.size(); i++) {
            Interval current = intervals.get(i);

            if (current.getLeft() - 1 <= end) {  // count for disjoint intervals to estimate time usage of step 2
                cntDisjointIntervals--;
            }

            if (current.getLeft() - 1 < end || (current.getLeft() - 1 == end && Double.compare(current.getEx(), ex) == 0 && Double.compare(current.getEx2(), ex2) == 0)) {
                end = Math.max(current.getRight(), end);
                ex = Math.min(current.getEx(), ex);
                ex2 = Math.min(current.getEx2(), ex2);
            } else {
                result.add(new Interval(start, end, ex, ex2));
                cntOffsets += end - start + 1;
                start = current.getLeft();
                end = current.getRight();
                ex = current.getEx();
                ex2 = current.getEx2();
            }
        }
        result.add(new Interval(start, end, ex, ex2));
        cntOffsets += end - start + 1;

        return new Pair<>(result, new Pair<>(cntDisjointIntervals, cntOffsets));
    }

    private List<Interval> sortAndMergeIntervals(List<Interval> intervals) {
        if (intervals.size() <= 1) {
            return intervals;
        }

        intervals.sort(Comparator.comparingLong(Interval::getLeft));

        Interval first = intervals.get(0);
        long start = first.getLeft();
        long end = first.getRight();
        double ex = first.getEx();
        double ex2 = first.getEx2();

        List<Interval> result = new ArrayList<>();

        for (int i = 1; i < intervals.size(); i++) {
            Interval current = intervals.get(i);
            if (current.getLeft() - 1 <= end) {
                end = Math.max(current.getRight(), end);
                ex = Math.min(current.getEx(), ex);
                ex2 = Math.min(current.getEx2(), ex2);
            } else {
                result.add(new Interval(start, end, ex, ex2));
                start = current.getLeft();
                end = current.getRight();
                ex = current.getEx();
                ex2 = current.getEx2();
            }
        }
        result.add(new Interval(start, end, ex, ex2));

        return result;
    }
}

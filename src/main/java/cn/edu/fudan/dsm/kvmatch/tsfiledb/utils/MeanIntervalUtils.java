package cn.edu.fudan.dsm.kvmatch.tsfiledb.utils;

import cn.edu.thu.tsfile.common.utils.Pair;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author Jiaye Wu
 */
public class MeanIntervalUtils {

    /**
     * The minimum value of time series, which should be adapt to the real data. (only for HBase)
     */
    private static double MINIMUM = 1000000;

    /**
     * Round float number to half integer.
     * For Example: 1.9 -&gt; 1.5,  1.4 -&gt; 1.0,  1.5 -&gt; 1.5, -1.9 -&gt; -2.0, -1.4 -&gt; -1.5, -1.5 -&gt; -1.5
     * TODO: should be improved later
     *
     * @param value should be rounded
     * @return rounded value
     */
    public static double toRound(double value) {
        double intValue = Math.floor(value);
        double diff = value - intValue;
        if (Double.compare(diff, 0.5) >= 0) {
            return intValue + 0.5;
        } else {
            return intValue;
        }
    }

    /**
     * toRound based on statistic information (lower bound)
     *
     * @param value         should be rounded
     * @param statisticInfo statistic information of index table
     * @return rounded value based on statistic information
     */
    public static double toRound(double value, List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) {
        double rounded = toRound(value);
        int index = Collections.binarySearch(statisticInfo, new Pair<>(rounded, 0), Comparator.comparing(o -> o.left));
        if (index < 0) {
            index = -(index + 1) - 1;
            if (index < 0) return rounded - 10000;
            return statisticInfo.get(index).left;
        } else {
            return rounded;
        }
    }

    /**
     * To upper bound of mean interval.
     * For example: 1.0 -&gt; 1.5,  1.5 -&gt; 2.0, -1.0 -&gt; -0.5, -1.5 -&gt; -1.0
     *
     * @param round mean interval round
     * @return upper bound
     */
    private static double toUpper(double round) {
        return round + 0.5;
    }

    /**
     * toUpper based on statistic information (upper bound)
     *
     * @param round         mean interval round
     * @param statisticInfo statistic information of index table
     * @return upper bound based on statistic information
     */
    public static double toUpper(double round, List<Pair<Double, Pair<Integer, Integer>>> statisticInfo) {
        double rounded = toUpper(round);
        int index = Collections.binarySearch(statisticInfo, new Pair<>(rounded, 0), Comparator.comparing(o -> o.left));
        if (index < 0) {
            index = -(index + 1);
            if (index >= statisticInfo.size()) return rounded + 10000;
            return statisticInfo.get(index).left;
        } else {
            return rounded;
        }
    }

    /**
     * Convert mean value into HBase row key.
     * 1. Add the minimum value, and convert to positive number;
     * 2. Round to 0.0 or 0.5;
     * 3. Convert to byte array.
     *
     * @param value should be processed
     * @return row key in bytes
     */
    public static byte[] toRoundBytes(double value) {
        return Bytes.toBytes(toRound(value) + MINIMUM);
    }

    public static byte[] toBytes(double value) {
        return Bytes.toBytes(value + MINIMUM);
    }

    /**
     * Convert HBase row key into mean value.
     * 1. Convert to double value;
     * 2. Minus the minimum value.
     *
     * @param bytes the bytes need to be converted
     * @return corresponding double value
     */
    public static double toDouble(byte[] bytes) {
        return Bytes.toDouble(bytes) - MINIMUM;
    }
}

package cn.edu.fudan.dsm.kvmatch.iotdb.statistic;

/**
 * Calculate mean value in stream fashion.
 *
 * @author Jiaye Wu
 */
public class StatisticInfo {

    private double sum;

    private long n;

    private double maximum;

    private double minimum;

    public StatisticInfo() {
        sum = 0;
        n = 0;
        maximum = Double.MIN_VALUE;
        minimum = Double.MAX_VALUE;
    }

    public void append(double value) {
        sum += value;
        n++;
        if (value > maximum) {
            maximum = value;
        }
        if (value < minimum) {
            minimum = value;
        }
    }

    public double getSum() {
        return sum;
    }

    public long getN() {
        return n;
    }

    public double getAverage() {
        return sum / n;
    }

    public double getMaximum() {
        return maximum;
    }

    public double getMinimum() {
        return minimum;
    }
}
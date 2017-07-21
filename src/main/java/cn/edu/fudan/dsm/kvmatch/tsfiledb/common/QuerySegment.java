package cn.edu.fudan.dsm.kvmatch.tsfiledb.common;

/**
 * @author Jiaye Wu
 */
public class QuerySegment {

    private double meanMin;

    private double meanMax;

    private int order;

    private int count;

    private int windowLength;

    public QuerySegment(double meanMin, double meanMax, int order, int count, int windowLength) {
        this.meanMin = meanMin;
        this.meanMax = meanMax;
        this.order = order;
        this.count = count;
        this.windowLength = windowLength;
    }

    @Override
    public String toString() {
        return String.valueOf(order) + "(" + String.valueOf(windowLength) + ")";
    }

    public double getMeanMin() {
        return meanMin;
    }

    public void setMeanMin(double meanMin) {
        this.meanMin = meanMin;
    }

    public double getMeanMax() {
        return meanMax;
    }

    public void setMeanMax(double meanMax) {
        this.meanMax = meanMax;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getWindowLength() {
        return windowLength;
    }

    public void setWindowLength(int windowLength) {
        this.windowLength = windowLength;
    }
}

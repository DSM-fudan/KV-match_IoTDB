package cn.edu.fudan.dsm.kvmatch.tsfiledb.common;

import cn.edu.thu.tsfile.common.utils.Pair;

import java.util.List;

/**
 * Configurations of KV-match querying.
 *
 * @author Jiaye Wu
 */
public class QueryConfig {

    private int windowLength = IndexConfig.DEFAULT_WINDOW_LENGTH;

    private List<Pair<Long, Double>> querySeries;

    private double epsilon;

    private double alpha;

    private double beta;

    public QueryConfig(List<Pair<Long, Double>> querySeries, double epsilon) {
        this.querySeries = querySeries;
        this.epsilon = epsilon;
    }

    public QueryConfig(List<Pair<Long, Double>> querySeries, double epsilon, double alpha, double beta) {
        this.querySeries = querySeries;
        this.epsilon = epsilon;
        this.alpha = alpha;
        this.beta = beta;
    }

    public List<Pair<Long, Double>> getQuerySeries() {
        return querySeries;
    }

    public void setQuerySeries(List<Pair<Long, Double>> querySeries) {
        this.querySeries = querySeries;
    }

    public double getEpsilon() {
        return epsilon;
    }

    public void setEpsilon(double epsilon) {
        this.epsilon = epsilon;
    }

    public double getAlpha() {
        return alpha;
    }

    public void setAlpha(double alpha) {
        this.alpha = alpha;
    }

    public double getBeta() {
        return beta;
    }

    public void setBeta(double beta) {
        this.beta = beta;
    }

    public int getWindowLength() {
        return windowLength;
    }

    public void setWindowLength(int windowLength) {
        this.windowLength = windowLength;
    }
}

package cn.edu.fudan.dsm.kvmatch.tsfiledb.common;

import java.util.List;

/**
 * Configurations of KV-match querying.
 *
 * @author Jiaye Wu
 */
public class QueryConfig {

    public static final double STEP_2_TIME_ESTIMATE_COEFFICIENT_A = 9.72276547123376;
    public static final double STEP_2_TIME_ESTIMATE_COEFFICIENT_B = 0.0106737255022236;
    public static final double STEP_2_TIME_ESTIMATE_INTERCEPT = 0.0;

    private List<Double> querySeries;

    private double epsilon;

    private double alpha;

    private double beta;

    private int windowLength;

    private boolean useCache;

    private QueryConfig() {
        this.windowLength = IndexConfig.DEFAULT_WINDOW_LENGTH;
        this.alpha = 1.0;
        this.beta = 0.0;
        this.useCache = true;
    }

    public QueryConfig(List<Double> querySeries, double epsilon) {
        this();
        this.querySeries = querySeries;
        this.epsilon = epsilon;
    }

    public QueryConfig(List<Double> querySeries, double epsilon, double alpha, double beta) {
        this(querySeries, epsilon);
        this.alpha = alpha;
        this.beta = beta;
    }

    public boolean isUseCache() {
        return useCache;
    }

    public void setUseCache(boolean useCache) {
        this.useCache = useCache;
    }

    public List<Double> getQuerySeries() {
        return querySeries;
    }

    public void setQuerySeries(List<Double> querySeries) {
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

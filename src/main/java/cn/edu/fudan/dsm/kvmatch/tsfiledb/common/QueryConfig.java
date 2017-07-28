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
    private boolean normalization;

    private boolean useCache;

    private IndexConfig indexConfig;

    private QueryConfig(IndexConfig indexConfig) {
        this.indexConfig = indexConfig;
        this.useCache = true;
        this.alpha = 1.0;
        this.beta = 0.0;
        this.normalization = false;
    }

    public QueryConfig(IndexConfig indexConfig, List<Double> querySeries, double epsilon) {
        this(indexConfig);
        this.querySeries = querySeries;
        this.epsilon = epsilon;
    }

    public QueryConfig(IndexConfig indexConfig, List<Double> querySeries, double epsilon, double alpha, double beta) {
        this(indexConfig, querySeries, epsilon);
        this.alpha = alpha;
        this.beta = beta;
        if (Double.compare(alpha, 1.0) != 0 || Double.compare(beta, 0.0) != 0) {
            this.normalization = true;
        }
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

    public boolean isNormalization() {
        return normalization;
    }

    public void setNormalization(boolean normalization) {
        this.normalization = normalization;
    }

    public IndexConfig getIndexConfig() {
        return indexConfig;
    }

    public void setIndexConfig(IndexConfig indexConfig) {
        this.indexConfig = indexConfig;
    }
}

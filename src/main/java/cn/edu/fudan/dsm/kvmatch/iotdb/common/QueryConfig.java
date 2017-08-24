package cn.edu.fudan.dsm.kvmatch.iotdb.common;

import cn.edu.tsinghua.tsfile.common.utils.Pair;

import java.util.ArrayList;
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

    // index basic information
    private IndexConfig indexConfig;

    // parameters
    private List<Double> querySeries;
    private double epsilon;
    private Pair<Long, Long> validTimeInterval;

    // statistics for RSM and cNSM problems
    private double meanQ;
    private double stdQ;

    // statistics for cNSM problem
    private boolean normalization;
    private double alpha;
    private double beta;
    private List<Double> normalizedQuerySeries;
    private List<Integer> order;

    // configurations for runtime optimization
    private boolean useCache;

    private QueryConfig(IndexConfig indexConfig) {
        this.indexConfig = indexConfig;
        this.useCache = true;
        this.alpha = 1.0;
        this.beta = 0.0;
        this.normalization = false;
        this.validTimeInterval = new Pair<>(0L, Long.MAX_VALUE);
    }

    public QueryConfig(IndexConfig indexConfig, List<Double> querySeries, double epsilon) {
        this(indexConfig);
        this.querySeries = querySeries;
        this.epsilon = epsilon;
    }

    public QueryConfig(IndexConfig indexConfig, List<Double> querySeries, double epsilon, double alpha, double beta, Pair<Long, Long> validTimeInterval) {
        this(indexConfig, querySeries, epsilon);
        this.alpha = alpha;
        this.beta = beta;
        if (Double.compare(alpha, 1.0) != 0 || Double.compare(beta, 0.0) != 0) {
            this.normalization = true;
        }
        this.validTimeInterval = validTimeInterval;
        calcAndSetStatistics();
    }

    private void calcAndSetStatistics() {
        // calculate required statistics
        int lenQ = querySeries.size();
        double ex = 0, ex2 = 0;
        for (double value : querySeries) {
            ex += value;
            ex2 += value * value;
        }
        meanQ = ex / lenQ;
        stdQ = Math.sqrt(ex2 / lenQ - meanQ * meanQ);

        if (normalization) {
            // do z-normalization on query data
            normalizedQuerySeries = new ArrayList<>(lenQ);
            order = new ArrayList<>(lenQ);
            for (double value : querySeries) {
                normalizedQuerySeries.add((value - meanQ) / stdQ);
            }
            // sort the query data
            List<Pair<Double, Integer>> tmpQuery = new ArrayList<>(lenQ);
            for (int i = 0; i < lenQ; i++) {
                tmpQuery.add(new Pair<>(normalizedQuerySeries.get(i), i));
            }
            tmpQuery.sort((o1, o2) -> o2.left.compareTo(o1.left));
            for (int i = 0; i < lenQ; i++) {
                normalizedQuerySeries.set(i, tmpQuery.get(i).left);
                order.add(tmpQuery.get(i).right);
            }
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

    public List<Double> getNormalizedQuerySeries() {
        return normalizedQuerySeries;
    }

    public void setNormalizedQuerySeries(List<Double> normalizedQuerySeries) {
        this.normalizedQuerySeries = normalizedQuerySeries;
    }

    public List<Integer> getOrder() {
        return order;
    }

    public void setOrder(List<Integer> order) {
        this.order = order;
    }

    public double getMeanQ() {
        return meanQ;
    }

    public void setMeanQ(double meanQ) {
        this.meanQ = meanQ;
    }

    public double getStdQ() {
        return stdQ;
    }

    public void setStdQ(double stdQ) {
        this.stdQ = stdQ;
    }

    public Pair<Long, Long> getValidTimeInterval() {
        return validTimeInterval;
    }

    public void setValidTimeInterval(Pair<Long, Long> validTimeInterval) {
        this.validTimeInterval = validTimeInterval;
    }

    public IndexConfig getIndexConfig() {
        return indexConfig;
    }

    public void setIndexConfig(IndexConfig indexConfig) {
        this.indexConfig = indexConfig;
    }
}

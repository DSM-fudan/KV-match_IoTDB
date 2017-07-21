package cn.edu.fudan.dsm.kvmatch.tsfiledb.common;

import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author Jiaye Wu
 */
public class IndexCache {

    private double beginRound;

    private double endRound;

    private NavigableMap<Double, IndexNode> caches;

    public IndexCache(double beginRound, double endRound) {
        this.beginRound = beginRound;
        this.endRound = endRound;
        caches = new ConcurrentSkipListMap<>();
    }

    public IndexCache(double beginRound, double endRound, NavigableMap<Double, IndexNode> caches) {
        this.beginRound = beginRound;
        this.endRound = endRound;
        this.caches = caches;
    }

    public void addCache(double meanRound, IndexNode indexNode) {
        caches.put(meanRound, indexNode);
    }

    public double getBeginRound() {
        return beginRound;
    }

    public void setBeginRound(double beginRound) {
        this.beginRound = beginRound;
    }

    public double getEndRound() {
        return endRound;
    }

    public void setEndRound(double endRound) {
        this.endRound = endRound;
    }

    public NavigableMap<Double, IndexNode> getCaches() {
        return caches;
    }

    public void setCaches(NavigableMap<Double, IndexNode> caches) {
        this.caches = caches;
    }
}

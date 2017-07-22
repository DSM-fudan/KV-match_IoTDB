package cn.edu.fudan.dsm.kvmatch.tsfiledb.common;

import cn.edu.thu.tsfile.common.utils.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Jiaye Wu
 */
public class QueryResult {

    private List<Pair<Long, Long>> candidateRanges;

    public QueryResult() {
        this.candidateRanges = new ArrayList<>();
    }

    public QueryResult(List<Pair<Long, Long>> candidateRanges) {
        this.candidateRanges = candidateRanges;
    }

    public List<Pair<Long, Long>> getCandidateRanges() {
        return candidateRanges;
    }

    public void setCandidateRanges(List<Pair<Long, Long>> candidateRanges) {
        this.candidateRanges = candidateRanges;
    }

    public void addCandidateRanges(List<Pair<Long, Long>> candidateRanges) {
        this.candidateRanges.addAll(candidateRanges);
    }
}

package cn.edu.fudan.dsm.kvmatch.tsfiledb.utils;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.Interval;
import cn.edu.thu.tsfile.common.utils.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Jiaye Wu
 */
public class IntervalUtils {

    public static List<Interval> sortButNotMergeIntervals(List<Interval> intervals) {
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

    public static Pair<List<Interval>, Pair<Integer, Long>> sortButNotMergeIntervalsAndCount(List<Interval> intervals) {
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

    public static List<Interval> sortAndMergeIntervals(List<Interval> intervals) {
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

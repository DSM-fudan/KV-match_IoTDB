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

    public static List<Pair<Long, Long>> extendAndMerge(List<Pair<Long, Long>> intervals, int extendLength) {
        for (Pair<Long, Long> interval : intervals) {
            if (interval.right != Long.MAX_VALUE) {
                interval.right += extendLength - 1;
            }
        }
        return mergePair(intervals);
    }

    public static List<Pair<Long, Long>> sortAndUnion(List<Pair<Long, Long>> intervals1, List<Pair<Long, Long>> intervals2) {
        intervals1.sort(Comparator.comparing(o -> o.left));
        intervals2.sort(Comparator.comparing(o -> o.left));
        return union(intervals1, intervals2);
    }

    public static List<Pair<Long,Long>> union(List<Pair<Long, Long>> intervals1, List<Pair<Long, Long>> intervals2) {
        List<Pair<Long, Long>> ret = new ArrayList<>();

        int index1 = 0, index2 = 0;
        Pair<Long, Long> last1 = null, last2 = null;
        while (index1 < intervals1.size() && index2 < intervals2.size()) {
            Pair<Long, Long> position1 = intervals1.get(index1);
            Pair<Long, Long> position2 = intervals2.get(index2);
            if (last1 == null) last1 = new Pair<>(position1.left, position1.right);
            if (last2 == null) last2 = new Pair<>(position2.left, position2.right);

            if (last1.right + 1 < last2.left) {
                ret.add(last1);
                index1++;
                last1 = null;
            } else if (last2.right + 1 < last1.left) {
                ret.add(last2);
                index2++;
                last2 = null;
            } else {
                if (last1.right < last2.right) {
                    if (last1.left < last2.left) {
                        last2.left = last1.left;
                    }
                    index1++;
                    last1 = null;
                } else {
                    if (last2.left < last1.left) {
                        last1.left = last2.left;
                    }
                    index2++;
                    last2 = null;
                }
            }
        }
        for (int i = index1; i < intervals1.size(); i++) {
            Pair<Long, Long> position1 = intervals1.get(i);
            if (last1 == null) last1 = new Pair<>(position1.left, position1.right);
            ret.add(last1);
            last1 = null;
        }
        for (int i = index2; i < intervals2.size(); i++) {
            Pair<Long, Long> position2 = intervals2.get(i);
            if (last2 == null) last2 = new Pair<>(position2.left, position2.right);
            ret.add(last2);
            last2 = null;
        }

        return ret;
    }

    public static List<Pair<Long, Long>> mergePair(List<Pair<Long, Long>> intervals) {
        if (intervals.size() <= 1) {
            return intervals;
        }

        Pair<Long, Long> first = intervals.get(0);
        long start = first.left;
        long end = first.right;

        List<Pair<Long, Long>> result = new ArrayList<>();

        for (int i = 1; i < intervals.size(); i++) {
            Pair<Long, Long> current = intervals.get(i);
            if (current.left - 1 <= end) {
                end = Math.max(current.right, end);
            } else {
                result.add(new Pair<>(start, end));
                start = current.left;
                end = current.right;
            }
        }
        result.add(new Pair<>(start, end));

        return result;
    }

    public static List<Pair<Long, Long>> sortAndMergePair(List<Pair<Long, Long>> intervals) {
        intervals.sort(Comparator.comparingLong(o -> o.left));
        return mergePair(intervals);
    }

    public static List<Interval> sortButNotMerge(List<Interval> intervals) {
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

    public static Pair<List<Interval>, Pair<Integer, Long>> sortButNotMergeAndCount(List<Interval> intervals) {
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

    public static List<Interval> sortAndMerge(List<Interval> intervals) {
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

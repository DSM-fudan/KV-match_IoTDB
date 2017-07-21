package cn.edu.fudan.dsm.kvmatch.tsfiledb.utils;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexNode;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.Pair;

/**
 * @author Jiaye Wu
 */
public class IndexNodeUtils {

    public static IndexNode mergeIndexNode(IndexNode node1, IndexNode node2) {
        IndexNode ret = new IndexNode();

        int index1 = 0, index2 = 0;
        Pair<Long, Long> last1 = null, last2 = null;
        while (index1 < node1.getPositions().size() && index2 < node2.getPositions().size()) {
            Pair<Long, Long> position1 = node1.getPositions().get(index1);
            Pair<Long, Long> position2 = node2.getPositions().get(index2);
            if (last1 == null) last1 = new Pair<>(position1.getFirst(), position1.getSecond());
            if (last2 == null) last2 = new Pair<>(position2.getFirst(), position2.getSecond());

            if (last1.getSecond() + 1 < last2.getFirst()) {
                addInterval(ret, last1);
                index1++;
                last1 = null;
            } else if (last2.getSecond() + 1 < last1.getFirst()) {
                addInterval(ret, last2);
                index2++;
                last2 = null;
            } else {
                if (last1.getSecond() < last2.getSecond()) {
                    if (last1.getFirst() < last2.getFirst()) {
                        last2.setFirst(last1.getFirst());
                    }
                    index1++;
                    last1 = null;
                } else {
                    if (last2.getFirst() < last1.getFirst()) {
                        last1.setFirst(last2.getFirst());
                    }
                    index2++;
                    last2 = null;
                }
            }
        }
        for (int i = index1; i < node1.getPositions().size(); i++) {
            Pair<Long, Long> position1 = node1.getPositions().get(i);
            if (last1 == null) last1 = new Pair<>(position1.getFirst(), position1.getSecond());
            addInterval(ret, last1);
            last1 = null;
        }
        for (int i = index2; i < node2.getPositions().size(); i++) {
            Pair<Long, Long> position2 = node2.getPositions().get(i);
            if (last2 == null) last2 = new Pair<>(position2.getFirst(), position2.getSecond());
            addInterval(ret, last2);
            last2 = null;
        }

        return ret;
    }

    private static void addInterval(IndexNode node, Pair<Long, Long> position) {
        // diff can not exceed 255
        while (position.getSecond() - position.getFirst() >= IndexNode.MAXIMUM_DIFF) {
            long newFirst = position.getFirst() + IndexNode.MAXIMUM_DIFF - 1;
            node.getPositions().add(new Pair<>(position.getFirst(), newFirst));
            position.setFirst(newFirst + 1);
        }
        // add last part
        node.getPositions().add(position);
    }
}

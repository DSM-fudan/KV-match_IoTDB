package cn.edu.fudan.dsm.kvmatch.tsfiledb.utils;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexNode;
import cn.edu.thu.tsfile.common.utils.Pair;

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
            if (last1 == null) last1 = new Pair<>(position1.left, position1.right);
            if (last2 == null) last2 = new Pair<>(position2.left, position2.right);

            if (last1.right + 1 < last2.left) {
                addInterval(ret, last1);
                index1++;
                last1 = null;
            } else if (last2.right + 1 < last1.left) {
                addInterval(ret, last2);
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
        for (int i = index1; i < node1.getPositions().size(); i++) {
            Pair<Long, Long> position1 = node1.getPositions().get(i);
            if (last1 == null) last1 = new Pair<>(position1.left, position1.right);
            addInterval(ret, last1);
            last1 = null;
        }
        for (int i = index2; i < node2.getPositions().size(); i++) {
            Pair<Long, Long> position2 = node2.getPositions().get(i);
            if (last2 == null) last2 = new Pair<>(position2.left, position2.right);
            addInterval(ret, last2);
            last2 = null;
        }

        return ret;
    }

    private static void addInterval(IndexNode node, Pair<Long, Long> position) {
        // diff can not exceed 255
        while (position.right - position.left >= IndexNode.MAXIMUM_DIFF) {
            long newFirst = position.left + IndexNode.MAXIMUM_DIFF - 1;
            node.getPositions().add(new Pair<>(position.left, newFirst));
            position.left = newFirst + 1;
        }
        // add last part
        node.getPositions().add(position);
    }
}

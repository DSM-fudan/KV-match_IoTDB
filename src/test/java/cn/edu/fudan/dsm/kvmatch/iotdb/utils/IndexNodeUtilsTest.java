package cn.edu.fudan.dsm.kvmatch.iotdb.utils;

import cn.edu.fudan.dsm.kvmatch.iotdb.common.IndexNode;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Jiaye Wu
 */
public class IndexNodeUtilsTest {

    @Test
    public void mergeIndexNode() throws Exception {
        IndexNode node1 = new IndexNode();
        IndexNode node2 = new IndexNode();

        node1.getPositions().add(new Pair<>(1L, 2L));  // 1, 2

        node2.getPositions().add(new Pair<>(4L, 5L));  // 4, 7
        node1.getPositions().add(new Pair<>(6L, 7L));

        node1.getPositions().add(new Pair<>(9L, 10L));  // 9, 16
        node2.getPositions().add(new Pair<>(11L, 12L));
        node1.getPositions().add(new Pair<>(13L, 14L));
        node2.getPositions().add(new Pair<>(15L, 16L));

        node1.getPositions().add(new Pair<>(18L, 20L));  // 18, 21
        node2.getPositions().add(new Pair<>(19L, 21L));

        node1.getPositions().add(new Pair<>(23L, 25L));  // 23, 26
        node2.getPositions().add(new Pair<>(24L, 26L));

        node1.getPositions().add(new Pair<>(30L, 31L));  // 30, 31

        node2.getPositions().add(new Pair<>(33L, 34L));  // 33, 34

        node1.getPositions().add(new Pair<>(36L, 37L));  // 36, 37

        node1.getPositions().add(new Pair<>(50L, 150L));  // 50, 305 | 306, 330
        node2.getPositions().add(new Pair<>(100L, 200L));
        node1.getPositions().add(new Pair<>(150L, 330L));

        node1.getPositions().add(new Pair<>(390L, 400L));  // 390, 400
        node1.getPositions().add(new Pair<>(420L, 430L));  // 420, 430
        node1.getPositions().add(new Pair<>(450L, 460L));  // 450, 460

        IndexNode merged = IndexNodeUtils.mergeIndexNode(node1, node2);
        assertEquals(new Pair<>(1L, 2L), merged.getPositions().get(0));
        assertEquals(new Pair<>(4L, 7L), merged.getPositions().get(1));
        assertEquals(new Pair<>(9L, 16L), merged.getPositions().get(2));
        assertEquals(new Pair<>(18L, 21L), merged.getPositions().get(3));
        assertEquals(new Pair<>(23L, 26L), merged.getPositions().get(4));
        assertEquals(new Pair<>(30L, 31L), merged.getPositions().get(5));
        assertEquals(new Pair<>(33L, 34L), merged.getPositions().get(6));
        assertEquals(new Pair<>(36L, 37L), merged.getPositions().get(7));
        assertEquals(new Pair<>(50L, 305L), merged.getPositions().get(8));
        assertEquals(new Pair<>(306L, 330L), merged.getPositions().get(9));
        assertEquals(new Pair<>(390L, 400L), merged.getPositions().get(10));
        assertEquals(new Pair<>(420L, 430L), merged.getPositions().get(11));
        assertEquals(new Pair<>(450L, 460L), merged.getPositions().get(12));
    }
}
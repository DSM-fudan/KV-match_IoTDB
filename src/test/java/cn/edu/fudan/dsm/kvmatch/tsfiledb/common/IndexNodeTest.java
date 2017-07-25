package cn.edu.fudan.dsm.kvmatch.tsfiledb.common;

import cn.edu.thu.tsfile.common.utils.Pair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Jiaye Wu
 */
public class IndexNodeTest {

    @Test
    public void test() {
        IndexNode node = new IndexNode();
        for (int i = 0; i < 100; i += 5) {
            node.getPositions().add(new Pair<>(i+1000000000000L, i+1000000000002L));
        }

        byte[] bytes = node.toBytesCompact();

        IndexNode node2 = new IndexNode();
        node2.parseBytesCompact(bytes);
        for (int i = 0; i < node2.getPositions().size(); i++) {
            assertEquals(node, node2);
        }
    }
}
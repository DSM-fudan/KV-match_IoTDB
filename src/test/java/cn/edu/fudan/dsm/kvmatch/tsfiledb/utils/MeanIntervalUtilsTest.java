package cn.edu.fudan.dsm.kvmatch.tsfiledb.utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Jiaye Wu
 */
public class MeanIntervalUtilsTest {

    @Test
    public void toRound() throws Exception {
        assertEquals(MeanIntervalUtils.toRound(-1.0), -1.0, 1e-10);
        assertEquals(MeanIntervalUtils.toRound(-0.9), -1.0, 1e-10);
        assertEquals(MeanIntervalUtils.toRound(-0.6), -1.0, 1e-10);
        assertEquals(MeanIntervalUtils.toRound(-0.5), -0.5, 1e-10);
        assertEquals(MeanIntervalUtils.toRound(-0.4), -0.5, 1e-10);
        assertEquals(MeanIntervalUtils.toRound(-0.1), -0.5, 1e-10);
        assertEquals(MeanIntervalUtils.toRound(0.0), 0.0, 1e-10);
        assertEquals(MeanIntervalUtils.toRound(0.1), 0.0, 1e-10);
        assertEquals(MeanIntervalUtils.toRound(0.4), 0.0, 1e-10);
        assertEquals(MeanIntervalUtils.toRound(0.5), 0.5, 1e-10);
        assertEquals(MeanIntervalUtils.toRound(0.6), 0.5, 1e-10);
        assertEquals(MeanIntervalUtils.toRound(0.9), 0.5, 1e-10);
        assertEquals(MeanIntervalUtils.toRound(1.0), 1.0, 1e-10);
    }
}
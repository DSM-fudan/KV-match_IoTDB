package cn.edu.fudan.dsm.kvmatch.tsfiledb.utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Jiaye Wu
 */
public class MeanIntervalUtilsTest {

    @Test
    public void toRound() throws Exception {
        assertEquals(-1.0, MeanIntervalUtils.toRound(-1.0), 1e-10);
        assertEquals(-1.0, MeanIntervalUtils.toRound(-0.9), 1e-10);
        assertEquals(-1.0, MeanIntervalUtils.toRound(-0.6), 1e-10);
        assertEquals(-0.5, MeanIntervalUtils.toRound(-0.5), 1e-10);
        assertEquals(-0.5, MeanIntervalUtils.toRound(-0.4), 1e-10);
        assertEquals(-0.5, MeanIntervalUtils.toRound(-0.1), 1e-10);
        assertEquals(0.0, MeanIntervalUtils.toRound(0.0), 1e-10);
        assertEquals(0.0, MeanIntervalUtils.toRound(0.1), 1e-10);
        assertEquals(0.0, MeanIntervalUtils.toRound(0.4), 1e-10);
        assertEquals(0.5, MeanIntervalUtils.toRound(0.5), 1e-10);
        assertEquals(0.5, MeanIntervalUtils.toRound(0.6), 1e-10);
        assertEquals(0.5, MeanIntervalUtils.toRound(0.9), 1e-10);
        assertEquals(1.0, MeanIntervalUtils.toRound(1.0), 1e-10);
    }
}
package cn.edu.fudan.dsm.kvmatch.tsfiledb.common;

import java.io.Serializable;

/**
 * Configurations of KV-match indexing.
 *
 * @author Jiaye Wu
 */
public class IndexConfig implements Serializable {

    public static final String PARAM_WINDOW_LENGTH = "window_length";

    public static final int DEFAULT_WINDOW_LENGTH = 50;

    public static final String PARAM_SINCE_TIME = "since_time";

    public static final long DEFAULT_SINCE_TIME = 0L;

    private int windowLength;

    private long sinceTime;

    public IndexConfig() {
        this.windowLength = DEFAULT_WINDOW_LENGTH;
        this.sinceTime = DEFAULT_SINCE_TIME;
    }

    public int getWindowLength() {
        return windowLength;
    }

    public void setWindowLength(int windowLength) {
        this.windowLength = windowLength;
    }

    public long getSinceTime() {
        return sinceTime;
    }

    public void setSinceTime(long sinceTime) {
        this.sinceTime = sinceTime;
    }
}

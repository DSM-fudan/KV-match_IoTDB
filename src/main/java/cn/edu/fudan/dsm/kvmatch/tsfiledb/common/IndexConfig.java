package cn.edu.fudan.dsm.kvmatch.tsfiledb.common;

/**
 * Configurations of KV-match indexing.
 *
 * @author Jiaye Wu
 */
public class IndexConfig {

    public static final String PARAM_WINDOW_LENGTH = "window_length";

    public static final int DEFAULT_WINDOW_LENGTH = 50;

    private int windowLength;

    public IndexConfig() {
        this.windowLength = DEFAULT_WINDOW_LENGTH;
    }

    public IndexConfig(int windowLength) {
        this.windowLength = windowLength;
    }

    public int getWindowLength() {
        return windowLength;
    }

    public void setWindowLength(int windowLength) {
        this.windowLength = windowLength;
    }
}

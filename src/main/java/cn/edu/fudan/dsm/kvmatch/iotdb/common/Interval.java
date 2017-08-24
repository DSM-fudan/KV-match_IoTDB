package cn.edu.fudan.dsm.kvmatch.iotdb.common;

/**
 * @author Jiaye Wu
 */
public class Interval {

    private long left;

    private long right;

    private double ex;

    private double ex2;

    public Interval(long left, long right, double ex, double ex2) {
        this.left = left;
        this.right = right;
        this.ex = ex;
        this.ex2 = ex2;
    }

    public long getLeft() {
        return left;
    }

    public void setLeft(long left) {
        this.left = left;
    }

    public long getRight() {
        return right;
    }

    public void setRight(long right) {
        this.right = right;
    }

    public double getEx() {
        return ex;
    }

    public void setEx(double ex) {
        this.ex = ex;
    }

    public double getEx2() {
        return ex2;
    }

    public void setEx2(double ex2) {
        this.ex2 = ex2;
    }

    @Override
    public String toString() {
        return "\n[" + String.valueOf((left - 1) * IndexConfig.DEFAULT_WINDOW_LENGTH + 1) + ", " +
                String.valueOf((right - 1) * IndexConfig.DEFAULT_WINDOW_LENGTH + 1) + "] - Ex: " + ex + ", Ex2: " + ex2;
    }
}

package cn.edu.fudan.dsm.kvmatch.tsfiledb.common;

import java.io.Serializable;

/**
 * A generic class for pairs.
 *
 * @param <T1>
 * @param <T2>
 * @author Jiaye Wu
 */
public class Pair<T1, T2> implements Serializable {

    private static final long serialVersionUID = -3986244606585552569L;

    private T1 first = null;
    private T2 second = null;

    public Pair() {

    }

    public Pair(T1 a, T2 b) {
        this.first = a;
        this.second = b;
    }

    public static <T1, T2> Pair<T1, T2> newPair(T1 a, T2 b) {
        return new Pair<>(a, b);
    }

    public void setFirst(T1 a) {
        this.first = a;
    }

    public void setSecond(T2 b) {
        this.second = b;
    }

    public T1 getFirst() {
        return first;
    }

    public T2 getSecond() {
        return second;
    }

    private static boolean equals(Object x, Object y) {
        return (x == null && y == null) || (x != null && x.equals(y));
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object other) {
        return other instanceof Pair &&
                equals(first, ((Pair) other).first) &&
                equals(second, ((Pair) other).second);
    }

    @Override
    public int hashCode() {
        if (first == null)
            return (second == null) ? 0 : second.hashCode() + 1;
        else if (second == null)
            return first.hashCode() + 2;
        else
            return first.hashCode() * 17 + second.hashCode();
    }

    @Override
    public String toString() {
        return "{" + getFirst() + "," + getSecond() + "}";
    }
}

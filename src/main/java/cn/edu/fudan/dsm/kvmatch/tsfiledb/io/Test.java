package cn.edu.fudan.dsm.kvmatch.tsfiledb.io;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dell on 2017/7/21.
 */
public class Test {

    public static void main(String[] args) {
        List<Long> array = new ArrayList<>();
        array.add(1L);
        array.add(2L);
        array.add(4L);
        array.add(5L);
        array.add(8L);
        array.add(9L);
        array.add(10L);
        //long x = lowerBound(8, array);
        //System.out.println(x);
        long x = upperBound(0, array);
        System.out.println(x);
    }

    private static long upperBound(int keyTo, List<Long> array) {
        int left = 0, right = 6;
        while (left <= right) {
            int mid = left + ((right-left)>>1);
            if (array.get(mid) <= keyTo) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        if (right >= 0) return array.get(right);
        return -1;
    }

    private static long lowerBound(double keyFrom, List<Long> array) {
        int left = 0, right = 6;
        while (left <= right) {
            int mid = left + ((right-left)>>1);
            if (array.get(mid) >= keyFrom) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        if (left < array.size()) return array.get(left);
        return -1;
    }

}

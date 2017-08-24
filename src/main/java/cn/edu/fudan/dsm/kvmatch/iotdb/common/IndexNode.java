package cn.edu.fudan.dsm.kvmatch.iotdb.common;

import cn.edu.fudan.dsm.kvmatch.iotdb.utils.Bytes;
import cn.edu.tsinghua.tsfile.common.utils.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * A node of the index.
 *
 * @author Jiaye Wu
 */
public class IndexNode {

    public static int MAXIMUM_DIFF = 256;

    private List<Pair<Long, Long>> positions;

    public IndexNode() {
        positions = new ArrayList<>(100);
    }

    public byte[] toBytes() {
        /*
         * {left 1}{right 1}{left 2}{right 2}...{left n}{right n}
         */
        byte[] result = new byte[Bytes.SIZEOF_LONG * positions.size() * 2];
        for (int i = 0; i < positions.size(); i++) {
            System.arraycopy(Bytes.toBytes(positions.get(i).left), 0, result, 2 * i * Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
            System.arraycopy(Bytes.toBytes(positions.get(i).right), 0, result, (2 * i + 1) * Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG);
        }
        return result;
    }

    public byte[] toBytesCompact() {
        /*
         * {left 1}{count 1}{right 1 - left 1}{left 2 - right 1}{right 2 - left 2}...{right count}{left count 1}...
         */
        byte[] result = new byte[Bytes.SIZEOF_LONG * positions.size() * 2];

        int index = 0, length = 0, count = 0;
        boolean isPacking = false;
        while (index < positions.size()) {
            if (!isPacking) {
                System.arraycopy(Bytes.toBytes(positions.get(index).left), 0, result, length, Bytes.SIZEOF_LONG);
                length += Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE;  // first: 4 bytes, remain 1 byte for count

                long diff = positions.get(index).right - positions.get(index).left;
                result[length++] = (byte) (diff - 128);

                isPacking = true;
                count = 1;
            } else {
                long diff = positions.get(index).left - positions.get(index - 1).right;
                if (diff < MAXIMUM_DIFF && (count - 1) / 2 + 2 < MAXIMUM_DIFF) {
                    result[length++] = (byte) (diff - 128);

                    diff = positions.get(index).right - positions.get(index).left;
                    result[length++] = (byte) (diff - 128);

                    count += 2;
                } else {
                    // write count
                    result[length - count - 1] = (byte) ((count - 1) / 2 - 128);

                    isPacking = false;
                    continue;
                }
            }
            index++;
        }
        if (isPacking) {  // write last count
            result[length - count - 1] = (byte) ((count - 1) / 2 - 128);
        }
        // TODO: resize array, use ByteBuffer instead?
        byte[] newArray = new byte[length];
        System.arraycopy(result, 0, newArray, 0, length);
        return newArray;
    }

    public void parseBytes(byte[] concatData) {
        if (concatData == null) return;
        byte[] tmp = new byte[Bytes.SIZEOF_LONG];
        positions.clear();
        for (int i = 0; i < concatData.length; i += 2 * Bytes.SIZEOF_LONG) {
            System.arraycopy(concatData, i, tmp, 0, Bytes.SIZEOF_LONG);
            long left = Bytes.toLong(tmp);
            System.arraycopy(concatData, i + Bytes.SIZEOF_LONG, tmp, 0, Bytes.SIZEOF_LONG);
            long right = Bytes.toLong(tmp);
            positions.add(new Pair<>(left, right));
        }
    }

    public void parseBytesCompact(byte[] concatData) {
        if (concatData == null) return;
        positions.clear();

        int index = 0;
        while (index < concatData.length) {
            byte[] tmp = new byte[Bytes.SIZEOF_LONG];
            System.arraycopy(concatData, index, tmp, 0, Bytes.SIZEOF_LONG);
            long left = Bytes.toLong(tmp);
            index += Bytes.SIZEOF_LONG;
            int count = (int) concatData[index++] + 128;
            long right = left + (int) concatData[index++] + 128;
            positions.add(new Pair<>(left, right));
            for (int i = 0; i < count; i++) {
                left = right + (int) concatData[index++] + 128;
                right = left + (int) concatData[index++] + 128;
                positions.add(new Pair<>(left, right));
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexNode indexNode = (IndexNode) o;

        return positions.equals(indexNode.positions);
    }

    @Override
    public int hashCode() {
        return positions.hashCode();
    }

    @Override
    public String toString() {
        return "IndexNode{" + "positions=" + positions + '}';
    }

    public List<Pair<Long, Long>> getPositions() {
        return positions;
    }

    public void setPositions(List<Pair<Long, Long>> positions) {
        this.positions = positions;
    }

    private int getNumOfIntervals() {
        return positions.size();
    }

    private int getNumOfOffsets() {
        int sum = 0;
        for (Pair<Long, Long> pair : positions) {
            sum += pair.right - pair.left + 1;
        }
        return sum;
    }

    public Pair<Integer, Integer> getStatisticInfoPair() {
        return new Pair<>(getNumOfIntervals(), getNumOfOffsets());
    }
}

package cn.edu.fudan.dsm.kvmatch.tsfiledb.io;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexNode;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.Pair;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.ByteUtils;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.utils.Bytes;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Index file reader for KV-match file version.
 *
 * @author Ningting Pan
 */
public class IndexFileReader implements Closeable {

    private File file;
    private RandomAccessFile reader;

    // Once open the file, read offsets
    List<Long> offsets = new ArrayList<>();

    public IndexFileReader(String indexFilePath) throws FileNotFoundException {
        this.file = new File(indexFilePath);
        this.reader = new RandomAccessFile(file, "r");
        // read offsets' info
        readOffsetInfo();
    }

    private void readOffsetInfo() {
        // read from the end of file
        try {
            byte[] bytes = new byte[Bytes.SIZEOF_LONG];
            reader.seek(file.length() - Bytes.SIZEOF_LONG);
            reader.read(bytes, 0, Bytes.SIZEOF_LONG);
            long lastLineOffset = Bytes.toLong(bytes);
            long length = file.length() - lastLineOffset;
            
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public Map<Double, List<IndexNode>> readIndexes(double keyFrom, double keyTo) {
        Map<Double, List<IndexNode>> indexes = null;

        return indexes;
    }

    public List<Pair<Double, Pair<Integer, Integer>>> readStatisticInfo() {
        List<Pair<Double, Pair<Integer, Integer>>> statisticInfo = new ArrayList<>();

        return statisticInfo;
    }

    private byte[] readALineBytesFromFile(int fromOffset, int lengthOfBytes) {

        return null;
    }

    @Override
    public void close() throws IOException {
        
    }
}

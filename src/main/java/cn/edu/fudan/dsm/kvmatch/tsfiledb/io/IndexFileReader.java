package cn.edu.fudan.dsm.kvmatch.tsfiledb.io;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.IndexNode;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.Pair;

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
    private BufferedInputStream reader;

    // Once open the file, read offsets
    List<Long> offsets = new ArrayList<>();

    public IndexFileReader(String indexFilePath) throws FileNotFoundException {
        this.file = new File(indexFilePath);
        this.reader = new BufferedInputStream(new FileInputStream(file));
        // read offsets' info
        readOffsetInfo();
    }

    private void readOffsetInfo() {
    }

    public Map<Double, List<IndexNode>> readIndexes() {
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

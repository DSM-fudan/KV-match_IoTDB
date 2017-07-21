package cn.edu.fudan.dsm.kvmatch.tsfiledb.io;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.entity.IndexNode;
import org.apache.hadoop.hbase.util.Pair;

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

    public IndexFileReader(File file) throws FileNotFoundException {
        this.file = file;
        this.reader = new BufferedInputStream(new FileInputStream(file));
    }

    public Map<Double, List<IndexNode>> readIndexes() {
        Map<Double, List<IndexNode>> indexes = null;

        return indexes;
    }

    public List<Pair<Double, Pair<Integer, Integer>>> readStatisticInfo() {
        List<Pair<Double, Pair<Integer, Integer>>> statisticInfo = new ArrayList<>();

        return statisticInfo;
    }
    
    @Override
    public void close() throws IOException {
        
    }
}

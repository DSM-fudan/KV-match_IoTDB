package cn.edu.fudan.dsm.kvmatch.tsfiledb.io;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.entity.IndexNode;
import org.apache.hadoop.hbase.util.Pair;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Index file reader for KV-match file version.
 *
 * @author Ningting Pan
 */
public class IndexFileReader implements Closeable {

    public static Map<Double, List<IndexNode>> read(File file, Double keyFrom, Double keyTo) {
        return null;
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

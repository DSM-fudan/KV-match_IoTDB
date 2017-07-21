package cn.edu.fudan.dsm.kvmatch.tsfiledb.io;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.entity.IndexNode;

import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * File input of KV-match-TsFileDB-index.
 *
 * @author Ningting Pan
 */
public class IndexFileWriter {

    public static void write(File file, Map<Double, List<IndexNode>> indexs) {
        int startOffset = 0;
        for (Map.Entry<Double, List<IndexNode>> entry : indexs.entrySet()) {
            Double key = entry.getKey();
            List<IndexNode> indexNodes = entry.getValue();
            byte[] keyBytes = ByteConvert.doubleToByteArray(key);
            byte[] indexNodesBytes = ByteConvert.listIndexNodeToByteArray(indexs);
            writeALineBytesToFile(file, keyBytes, indexNodesBytes);
        }

    }

    private static void writeALineBytesToFile(File file, byte[] keyBytes, byte[] indexNodesBytes) {
        try {
            FileInputStream fread = new FileInputStream(file);
            BufferedOutputStream writer = new BufferedOutputStream(new FileOutputStream(file));


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

}

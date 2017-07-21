package cn.edu.fudan.dsm.kvmatch.tsfiledb;

import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.QueryConfig;
import cn.edu.fudan.dsm.kvmatch.tsfiledb.common.QueryResult;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;

import java.util.concurrent.Callable;

/**
 * This is the class actually execute the KV-match index query processing for one index file.
 *
 * @author Jiaye Wu
 */
public class KvMatchQueryExecutor implements Callable<QueryResult> {

    private QueryConfig queryConfig;

    private Path columnPath;

    private String indexFilePath;

    public KvMatchQueryExecutor(QueryConfig queryConfig, Path columnPath, String indexFilePath) {
        this.queryConfig = queryConfig;
        this.columnPath = columnPath;
        this.indexFilePath = indexFilePath;
    }

    @Override
    public QueryResult call() throws Exception {
        return new QueryResult();
    }
}

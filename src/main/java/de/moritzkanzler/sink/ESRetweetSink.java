package de.moritzkanzler.sink;

import de.moritzkanzler.helper.ESClient;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class ESRetweetSink implements ElasticsearchSinkFunction<Tuple2<Long, Integer>> {

    private Logger logger;
    private String indexName;
    private String typeName;

    public ESRetweetSink(Logger logger, String indexName, String typeName) {
        this.logger = logger;
        this.indexName = indexName;
        this.typeName = typeName;
    }

    @Override
    public void process(Tuple2<Long, Integer> t, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("watermark", t.f0)
                    .field("cnt", t.f1)
                    .endObject();

            IndexRequest indexRequest = ESClient
                    .getInstance()
                    .requestIndex(indexName, typeName, builder);
            requestIndexer.add(indexRequest);
        } catch (Exception e) {

        }
    }
}

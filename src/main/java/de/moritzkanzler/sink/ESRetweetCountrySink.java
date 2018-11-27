package de.moritzkanzler.sink;

import de.moritzkanzler.helper.ESClient;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class ESRetweetCountrySink implements ElasticsearchSinkFunction<Tuple5<String, Double, Double, Long, Integer>> {

    private Logger logger;
    private String indexName;
    private String typeName;

    public ESRetweetCountrySink(Logger logger, String indexName, String typeName) {
        this.logger = logger;
        this.indexName = indexName;
        this.typeName = typeName;
    }

    @Override
    public void process(Tuple5<String, Double, Double, Long, Integer> t, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("langcode", t.f0)
                    .startObject("location")
                    .field("lat", t.f1)
                    .field("lon", t.f2)
                    .endObject()
                    .field("watermark", t.f3)
                    .field("cnt", t.f4)
                    .endObject();

            IndexRequest indexRequest = ESClient
                    .getInstance()
                    .requestIndex(indexName, typeName, builder);
            requestIndexer.add(indexRequest);
        } catch (Exception e) {

        }
    }
}

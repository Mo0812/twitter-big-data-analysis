package de.moritzkanzler.sink;

import de.moritzkanzler.helper.ESClient;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;

public class ESTweetRetweetShareCountry implements ElasticsearchSinkFunction<Tuple5<String, Long, Integer, Integer, Double>> {

    private Logger logger;
    private String indexName;
    private String typeName;

    public ESTweetRetweetShareCountry(Logger logger, String indexName, String typeName) {
        this.logger = logger;
        this.indexName = indexName;
        this.typeName = typeName;
    }

    @Override
    public void process(Tuple5<String, Long, Integer, Integer, Double> t, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("langcode", t.f0)
                    .field("watermark", t.f1)
                    .field("rt_total", t.f2)
                    .field("tw_total", t.f3)
                    .field("percentage", t.f4)
                    .endObject();

            IndexRequest indexRequest = ESClient
                    .getInstance()
                    .requestIndex(indexName, typeName, builder);
            requestIndexer.add(indexRequest);
        } catch (Exception e) {

        }
    }
}

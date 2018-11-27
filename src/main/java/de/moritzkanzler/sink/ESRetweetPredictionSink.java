package de.moritzkanzler.sink;

import de.moritzkanzler.helper.ESClient;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;

public class ESRetweetPredictionSink implements ElasticsearchSinkFunction<Tuple5<String, Long, Integer, Double, Double>> {

    private Logger logger;
    private String indexName;
    private String typeName;

    public ESRetweetPredictionSink(Logger logger, String indexName, String typeName) {
        this.logger = logger;
        this.indexName = indexName;
        this.typeName = typeName;
    }

    @Override
    public void process(Tuple5<String, Long, Integer, Double, Double> t, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("prediction_type", t.f0)
                    .field("watermark", t.f1)
                    .field("cnt", t.f2)
                    .field("prediction", t.f3)
                    .field("deviation", t.f4)
                    .endObject();

            IndexRequest indexRequest = ESClient
                    .getInstance()
                    .requestIndex(indexName, typeName, builder);
            requestIndexer.add(indexRequest);
        } catch (Exception e) {

        }
    }
}

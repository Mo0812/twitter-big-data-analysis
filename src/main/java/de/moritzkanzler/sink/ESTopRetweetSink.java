package de.moritzkanzler.sink;

import de.moritzkanzler.helper.ESClient;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class ESTopRetweetSink implements ElasticsearchSinkFunction<Tuple5<Long, String, String, Long, Integer>> {

    private Logger logger;
    private String indexName;
    private String typeName;

    public ESTopRetweetSink(Logger logger, String indexName, String typeName) {
        this.logger = logger;
        this.indexName = indexName;
        this.typeName = typeName;
    }

    @Override
    public void process(Tuple5<Long, String, String, Long, Integer> t, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        Map<String, String> json = new HashMap<>();
        json.put("tId", t.f0.toString());
        json.put("screenname", t.f1);
        json.put("tweet", t.f2);
        json.put("watermark", t.f3.toString());
        json.put("cnt", t.f4.toString());

        try {
            IndexRequest indexRequest = ESClient
                    .getInstance()
                    .requestIndex(indexName, typeName, json);
            requestIndexer.add(indexRequest);
        } catch(Exception e) {
            logger.warn("Not indexed information here");
        }
    }
}

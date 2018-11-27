package de.moritzkanzler.helper;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is a wrapper for establishing a elasticsearch sink for flink, so that the fetching of properties and building of the sink don't need to be implemented repeatedly in the flink jobs
 */
public class ESConnection {

    /**
     * Get property out of the ES property file
     * @param name
     * @return
     * @throws Exception
     */
    private static String getProperty(String name) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("src/main/resources/elasticsearch.properties");

        if (!parameterTool.has(name)) {
            throw new Exception("Value not defined in properties.");
        } else {
            return parameterTool.get(name);
        }
    }

    /**
     * Get bulkFlashMaxActions for the sink out of ES property file
     * @return
     * @throws Exception
     */
    public static int getBulkFlashMaxActions() throws Exception {
        return Integer.parseInt(ESConnection.getProperty("elasticsearch.bluk.flush.max.actions"));
    }

    /**
     * Return transport informations for the sink out of the ES property file
     * @return
     * @throws Exception
     */
    public static List<HttpHost> getTransportList() throws Exception {
        List<HttpHost> transport = new ArrayList<>();
        transport.add(new HttpHost(ESConnection.getProperty("elasticsearch.host"), Integer.parseInt(ESConnection.getProperty("elasticsearch.port.rest")), ESConnection.getProperty("elasticsearch.protocol")));
        transport.add(new HttpHost(ESConnection.getProperty("elasticsearch.host"), Integer.parseInt(ESConnection.getProperty("elasticsearch.port.rest")), ESConnection.getProperty("elasticsearch.protocol")));
        return transport;
    }

    /**
     * Method to build a ES sink for an apache flink environment
     * @param esSinkFunction
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> ElasticsearchSink<T> buildESSink(ElasticsearchSinkFunction<T> esSinkFunction) throws Exception {
        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<>(
                ESConnection.getTransportList(),
                esSinkFunction
        );
        esSinkBuilder.setBulkFlushMaxActions(ESConnection.getBulkFlashMaxActions());
        return esSinkBuilder.build();
    }
}

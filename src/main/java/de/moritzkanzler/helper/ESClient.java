package de.moritzkanzler.helper;

import org.apache.flink.api.java.utils.ParameterTool;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * Singleton class which enables to communicate with a set elasticsearch instance
 */
public class ESClient {

    private static Logger logger = LoggerFactory.getLogger(ESClient .class);
    private static ESClient instance = null;

    private TransportClient client;
    private IndicesAdminClient indicesAdminClient;

    /**
     * Singleton mechanism
     * @return ESClient
     * @throws UnknownHostException
     * @throws Exception
     */
    public static ESClient getInstance() throws UnknownHostException, Exception {
        if(instance == null) {
            instance = new ESClient();
        }
        return instance;
    }

    /**
     * Constructor which recieves the elasticsearch connection details from a property file and builds the ES client and the adminClient
     * @throws UnknownHostException
     * @throws Exception
     */
    protected ESClient() throws UnknownHostException, Exception {
        try {
            ParameterTool parameterTool = ParameterTool.fromPropertiesFile("src/main/resources/elasticsearch.properties");

            if(!parameterTool.has("elasticsearch.cluster.name")) {
                throw new Exception("No cluster name defined");
            }
            if(!parameterTool.has("elasticsearch.host")) {
                throw new UnknownHostException("No host defined");
            }
            if(!parameterTool.has("elasticsearch.port.command")) {
                throw new Exception("No port defined");
            }

            Settings settings = Settings.builder().put("cluster.name", parameterTool.get("elasticsearch.cluster.name")).build();
            this.client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(parameterTool.get("elasticsearch.host")), Integer.parseInt(parameterTool.get("elasticsearch.port.command"))));

            this.indicesAdminClient = client.admin().indices();
        } catch (Exception e) {
            logger.error("Elasticsearch parameter properties not found");
        }
    }

    /**
     * Checks if a index already exists
     * @param indexName
     * @return
     */
    private boolean indexExists(String indexName) {
        return this.indicesAdminClient.prepareExists(indexName).execute().actionGet().isExists();
    }

    /**
     * Checks if a type already exists
     * @param indexName
     * @param typeName
     * @return
     */
    private boolean typeExists(String indexName, String typeName) {
        return this.indicesAdminClient.typesExists(new TypesExistsRequest(new String[] {indexName}, typeName)).actionGet().isExists();
    }

    /**
     * Create a given index in the ES instance
     * @param indexName
     */
    public void buildIndex(String indexName) {
        if(!this.indexExists(indexName)) {
            this.indicesAdminClient.prepareCreate(indexName).get();
        }
    }

    /**
     * Create a given index in the ES instance and deletes it if this index already exists
     * @param indexName
     * @param deleteFirst
     */
    public void buildIndex(String indexName, boolean deleteFirst) {
        if(deleteFirst && indexExists(indexName)) {
            this.deleteIndex(indexName);
        }
        this.buildIndex(indexName);
    }

    /**
     * Delete an given index
     * @param indexName
     */
    public void deleteIndex(String indexName) {

        DeleteIndexResponse deleteResponse = this.indicesAdminClient.delete(new DeleteIndexRequest(indexName)).actionGet();
    }

    /**
     * Builds a given mapping structure to a given index and type in ES
     * @param indexName
     * @param typeName
     * @param properties mapping structure
     */
    public void buildMapping(String indexName, String typeName, XContentBuilder properties) {
        if(!this.typeExists(indexName, typeName)) {
            this.indicesAdminClient.preparePutMapping(indexName)
                    .setType(typeName)
                    .setSource(properties)
                    .get();
        }
    }

    /**
     * Prepares a IndexRequest object to operate with the given index and type in ES and also put in data later
     * @param indexName
     * @param typeName
     * @param json
     * @return
     */
    public IndexRequest requestIndex(String indexName, String typeName, Map<String, String> json) {
        return this.client.prepareIndex(indexName, typeName).setSource(json).request();
    }

    /**
     * Prepares a IndexRequest object to operate with the given index and type in ES and also put in data later
     * @param indexName
     * @param typeName
     * @param json
     * @return
     */
    public IndexRequest requestIndex(String indexName, String typeName, XContentBuilder json) {
        return this.client.prepareIndex(indexName, typeName).setSource(json).request();
    }

    @Deprecated
    public void requestIndexData(String indexName, String typeName, Map<String, String> json) {
        IndexResponse respones = this.client.prepareIndex(indexName, typeName).setSource(json).get();
        logger.info(respones.status().toString());
    }
}
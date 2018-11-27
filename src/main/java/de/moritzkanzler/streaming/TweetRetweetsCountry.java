package de.moritzkanzler.streaming;

import de.moritzkanzler.exception.GeoCoordinatesNotFoundException;
import de.moritzkanzler.filter.IgnoreDeletedPostsFilter;
import de.moritzkanzler.helper.*;
import de.moritzkanzler.keyBy.KeyByTweetPostId;
import de.moritzkanzler.map.RetweetLangFlatMap;
import de.moritzkanzler.map.TweetCountryCsvMap;
import de.moritzkanzler.map.TwitterModelMap;
import de.moritzkanzler.model.TwitterPostData;
import de.moritzkanzler.sink.ESRetweetCountrySink;
import de.moritzkanzler.sink.ESRetweetPercentageCountrySink;
import de.moritzkanzler.utils.GeoCoordinates;
import de.moritzkanzler.utils.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static de.moritzkanzler.helper.JobType.STREAM;

/**
 * METRIC 3 & 4
 */
public class TweetRetweetsCountry {

    /*
    Init Logger and Parameters
     */
    static Logger logger = LoggerFactory.getLogger(TweetTopXRetweet.class);
    static JobParameters paramTool = JobParameters.getInstance("src/main/resources/programs/tweetretweetscountry.properties");
    static String indexNameGeo = paramTool.getProperty(STREAM, "geo.elasticsearch.index", "");
    static String typeNameGeo = paramTool.getProperty(STREAM, "geo.elasticsearch.type", "");
    static String indexNamePercentage = paramTool.getProperty(STREAM, "percentage.elasticsearch.index", "");
    static String typeNamePercentage = paramTool.getProperty(STREAM, "percentage.elasticsearch.type", "");
    static boolean deleteIndex = paramTool.getProperty(STREAM, "elasticsearch.delete", false);
    static boolean esOutput = paramTool.getProperty(STREAM, "elasticsearch.output", false);
    static boolean csvOutput = paramTool.getProperty(STREAM, "csv.output", false);
    static String csvPath = paramTool.getProperty(STREAM, "dump.csv.path", "");
    static Time windowSize = Time.minutes(15);
    static Time windowAllSize = Time.minutes(15);
    static Integer decimalPlace = paramTool.getProperty(STREAM, "general.decimalplace", 0);

    /**
     * Process stream job
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        // Establish twitter connection
        TwitterSource twitterSource = TwitterConnection.getTwitterConnection();

        if(esOutput) {
            prepareES(indexNameGeo, typeNameGeo, indexNamePercentage, typeNamePercentage, deleteIndex);
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Add twitter as source
        DataStream<String> streamSource = env.addSource(twitterSource);

        // Filter deleted posts and map input to Twitter model
        DataStream<TwitterPostData> tweets = streamSource
                .filter(new IgnoreDeletedPostsFilter())
                .map(new TwitterModelMap());

        // ---

        // Get retweets out of all tweets per language
        DataStream<Tuple2<String, Integer>> retweetsLangMap = tweets
                .keyBy(new KeyByTweetPostId())
                .flatMap(new RetweetLangFlatMap());

        // Sum up retweets per language
        DataStream<Tuple2<String, Integer>> retweetsLangSum = retweetsLangMap
                .keyBy(0)
                .timeWindow(windowSize)
                .sum(1);

        // ---

        /*
         METRIC 4: Calculate percentage of retweets per country
        */
        DataStream<Tuple4<String, Long, Double, Integer>> retweetsLangPercentage = retweetsLangSum
                .keyBy(0)
                .timeWindowAll(windowAllSize)
                .process(new RetweetCountryPercentageProcessAll());

        // ---

        /*
        METRIC 3: Add Geodata to tweets per language
        */
        DataStream<Tuple5<String, Double, Double, Long, Integer>> retweetsLangGeo = retweetsLangSum
                .keyBy(0)
                .timeWindowAll(windowAllSize)
                .process(new RetweetCountryProcessAll());

        /*
        Elasticsearch output
         */
        if(esOutput) {
            ElasticsearchSink<Tuple5<String, Double, Double, Long, Integer>> esSink = ESConnection.buildESSink(new ESRetweetCountrySink(logger, indexNameGeo, typeNameGeo));
            retweetsLangGeo.addSink(esSink);

            ElasticsearchSink<Tuple4<String, Long, Double, Integer>> esSinkLangPercentage = ESConnection.buildESSink(new ESRetweetPercentageCountrySink(logger, indexNamePercentage, typeNamePercentage));
            retweetsLangPercentage.addSink(esSinkLangPercentage);
        }
        /*
        CSV output
         */
        if(csvOutput) {
            DataStream<Tuple4<Long, String, Long, Long>> csvOutput = tweets
                    .flatMap(new TweetCountryCsvMap());
            csvOutput.writeAsCsv(csvPath, FileSystem.WriteMode.OVERWRITE);
        }

        retweetsLangPercentage.print();
        retweetsLangGeo.print();

        env.execute("Twitter Retweets per Country");
    }

    /**
     * Prepration of ES index to save data later on
     * @throws Exception
     */
    private static void prepareES(String indexNameGeo, String typeNameGeo, String indexNamePercentage, String typeNamePercentage, boolean delete) throws Exception {

        //Prepare ES Index for METRIC 3
        XContentBuilder esProperties = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("properties")
                    .startObject("langcode")
                        .field("type", "text")
                    .endObject()
                    .startObject("location")
                        .field("type", "geo_point")
                    .endObject()
                    .startObject("watermark")
                        .field("type", "date")
                    .endObject()
                    .startObject("cnt")
                        .field("type", "integer")
                    .endObject()
                .endObject()
            .endObject();

        // Manage Index in ES for METRIC 3
        ESClient esClient = ESClient.getInstance();
        esClient.buildIndex(indexNameGeo, delete);
        esClient.buildMapping(indexNameGeo, typeNameGeo, esProperties);


        //Prepare ES Index for METRIC 4
        XContentBuilder esProperties2 = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                        .startObject("langcode")
                            .field("type", "keyword")
                        .endObject()
                        .startObject("watermark")
                            .field("type", "date")
                        .endObject()
                        .startObject("percentage")
                            .field("type","double")
                        .endObject()
                        .startObject("cnt")
                            .field("type", "integer")
                        .endObject()
                    .endObject()
                .endObject();

        // Manage Index in ES for METRIC 4
        esClient.buildIndex(indexNamePercentage, delete);
        esClient.buildMapping(indexNamePercentage, typeNamePercentage, esProperties2);
    }

    /**
     * Process function which applied the coordinates of the capital city of a country to the given language code. If no matching coordinates can be found, it will be initalized with latitude 0.0 and longitude 0.0 and also prints a warning
     */
    public static class RetweetCountryProcessAll extends ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple5<String, Double, Double, Long, Integer>, TimeWindow> {
        @Override
        public void process(Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple5<String, Double, Double, Long, Integer>> collector) throws Exception {
            for(Tuple2<String, Integer> countryRetweets: iterable) {
                try {
                    GeoCoordinates geo = Utils.langCodeToCoords(countryRetweets.f0);
                    collector.collect(new Tuple5<>(countryRetweets.f0, geo.getLatitude(), geo.getLongitude(), context.window().getStart(), countryRetweets.f1));
                } catch(GeoCoordinatesNotFoundException e) {
                    logger.warn("Language code: " + countryRetweets.f0 + " not found");
                    collector.collect(new Tuple5<>(countryRetweets.f0, 0.0, 0.0, context.window().getStart(), countryRetweets.f1));
                }
            }
        }
    }

    /**
     * Process function for the generation of share of retweets per country according to the total amount of retweets
     */
    public static class RetweetCountryPercentageProcessAll extends ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple4<String, Long, Double, Integer>, TimeWindow> {
        @Override
        public void process(Context context, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple4<String, Long, Double, Integer>> collector) throws Exception {
            int total = 0;
            for(Tuple2<String, Integer> countryRetweets: iterable) {
                total += countryRetweets.f1;
            }
            for(Tuple2<String, Integer> countryRetweets: iterable) {
                Double percentage = (double) countryRetweets.f1 / total * 100;
                percentage = Utils.roundDouble(percentage, decimalPlace);
                collector.collect(new Tuple4<>(countryRetweets.f0, context.window().getStart(), percentage, countryRetweets.f1));
            }
        }
    }
}

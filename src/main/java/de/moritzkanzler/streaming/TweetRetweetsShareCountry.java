package de.moritzkanzler.streaming;

import de.moritzkanzler.filter.IgnoreDeletedPostsFilter;
import de.moritzkanzler.helper.ESClient;
import de.moritzkanzler.helper.ESConnection;
import de.moritzkanzler.helper.JobParameters;
import de.moritzkanzler.helper.TwitterConnection;
import de.moritzkanzler.keyBy.KeyByTweetPostId;
import de.moritzkanzler.map.RetweetLangFlatMap;
import de.moritzkanzler.map.TweetCountryCsvMap;
import de.moritzkanzler.map.TweetLangFlatMap;
import de.moritzkanzler.map.TwitterModelMap;
import de.moritzkanzler.model.TwitterPostData;
import de.moritzkanzler.sink.ESTweetRetweetShareCountry;
import de.moritzkanzler.streaming.TweetTopXRetweet;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

import static de.moritzkanzler.helper.JobType.STREAM;

/**
 * METRIC 5: Share of retweets regarding to all tweets for each country
 */
public class TweetRetweetsShareCountry {

    /*
    Init Logger and Parameters
    */
    static Logger logger = LoggerFactory.getLogger(TweetTopXRetweet.class);
    static JobParameters paramTool = JobParameters.getInstance("src/main/resources/programs/tweetretweetssharecountry.properties");
    static String indexName = paramTool.getProperty(STREAM, "elasticsearch.index", "");
    static String typeName = paramTool.getProperty(STREAM, "elasticsearch.type", "");
    static boolean deleteIndex = paramTool.getProperty(STREAM, "elasticsearch.delete", false);
    static boolean esOutput = paramTool.getProperty(STREAM, "elasticsearch.output", false);
    static boolean csvOutput = paramTool.getProperty(STREAM, "csv.output", false);
    static String csvOutputPath = paramTool.getProperty(STREAM, "out.csv.path", "");
    static String csvPath = paramTool.getProperty(STREAM, "dump.csv.path", "");
    static Time windowSize = Time.seconds(15);
    static Time windowAllSize = Time.seconds(15);
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
            prepareES(indexName, typeName, deleteIndex);
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Add twitter as source
        DataStream<String> streamSource = env.addSource(twitterSource);

        // Filter deleted posts and map input to Twitter model
        DataStream<TwitterPostData> tweets = streamSource
                .filter(new IgnoreDeletedPostsFilter())
                .map(new TwitterModelMap());

        // ---

        // Sum up tweets per language
        DataStream<Tuple2<String, Integer>> tweetsLangSum = tweets
                .assignTimestampsAndWatermarks(new TweetWatermarkAssigner())
                .flatMap(new TweetLangFlatMap())
                .keyBy(0)
                .timeWindow(windowSize)
                .sum(1);

        // ---

        // Get retweets out of all tweets per language
        DataStream<Tuple2<String, Integer>> retweetsLangMap = tweets
                .assignTimestampsAndWatermarks(new TweetWatermarkAssigner())
                .keyBy(new KeyByTweetPostId())
                .flatMap(new RetweetLangFlatMap());

        // Sum up retweets per language
        DataStream<Tuple2<String, Integer>> retweetsLangSum = retweetsLangMap
                .keyBy(0)
                .timeWindow(windowSize)
                .sum(1);

        // ---

        DataStream<Tuple5<String, Long, Integer, Integer, Double>> tweetRetweetShare = tweetsLangSum.join(retweetsLangSum)
                .where(new KeyByTweetTuple())
                .equalTo(new KeyByTweetTuple())
                .window(TumblingEventTimeWindows.of(windowSize))
                .apply(new TweetPercentageJoin())
                .timeWindowAll(windowAllSize)
                .process(new MaxCountryShareProcessAll());

        /*
        Elasticsearch output
         */
        if(esOutput) {
            ElasticsearchSink<Tuple5<String, Long, Integer, Integer, Double>> esSink = ESConnection.buildESSink(new ESTweetRetweetShareCountry(logger, indexName, typeName));
            tweetRetweetShare.addSink(esSink);
        }
        /*
        CSV output
         */
        if(csvOutput) {
            DataStream<Tuple4<Long, String, Long, Long>> csvOutput = tweets
                    .flatMap(new TweetCountryCsvMap());
            csvOutput.writeAsCsv(csvPath, FileSystem.WriteMode.OVERWRITE);

            tweetRetweetShare.writeAsCsv(csvOutputPath, FileSystem.WriteMode.OVERWRITE);
        } else {
            tweetRetweetShare.print();
        }

        env.execute("Twitter Tweet Retweet Share Country");
    }

    /**
     * Prepration of ES index to save data later on
     * @throws Exception
     */
    public static void prepareES(String indexName, String typeName, boolean delete) throws Exception{
        //Prepare ES Index
        XContentBuilder esProperties = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("langcode")
                .field("type", "keyword")
                .endObject()
                .startObject("watermark")
                .field("type", "date")
                .endObject()
                .startObject("rt_total")
                .field("type", "integer")
                .endObject()
                .startObject("tw_total")
                .field("type", "integer")
                .endObject()
                .startObject("percentage")
                .field("type", "double")
                .endObject()
                .endObject()
                .endObject();

        // Manage Index in ES
        ESClient esClient = ESClient.getInstance();
        esClient.buildIndex(indexName, delete);
        esClient.buildMapping(indexName, typeName, esProperties);
    }

    /**
     * Assigning watermark for later usage
     */
    public static class TweetWatermarkAssigner implements AssignerWithPeriodicWatermarks<TwitterPostData> {
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis() - 5000);
        }

        @Override
        public long extractTimestamp(TwitterPostData twitterPostData, long l) {
            return twitterPostData.getWatermark();
        }
    }

    /**
     * KeySelector for tweet ID
     */
    public static class KeyByTweetTuple implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> tweet) throws Exception {
            return tweet.f0;
        }
    }

    /**
     * Join function two calculate retweets against tweets per country
     */
    public static class TweetPercentageJoin implements JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple4<String, Integer, Integer, Double>> {
        @Override
        public Tuple4<String, Integer, Integer, Double> join(Tuple2<String, Integer> tweet, Tuple2<String, Integer> retweet) throws Exception {
            String lang = retweet.f0;
            Double percentage = (double)  retweet.f1 / tweet.f1;
            return new Tuple4<>(lang, retweet.f1, tweet.f1, percentage);
        }
    }

    /**
     * Processes the single calculations per country to select the max entry per time window
     */
    public static class MaxCountryShareProcessAll extends ProcessAllWindowFunction<Tuple4<String, Integer, Integer, Double>, Tuple5<String, Long, Integer, Integer, Double>, TimeWindow> {
        @Override
        public void process(Context context, Iterable<Tuple4<String, Integer, Integer, Double>> iterable, Collector<Tuple5<String, Long, Integer, Integer, Double>> collector) throws Exception {
            Map<String, Tuple4<String, Integer, Integer, Double>> maxList = new HashMap<>();
            for(Tuple4<String, Integer, Integer, Double> newVal: iterable) {
                if(maxList.containsKey(newVal.f0)) {
                    Tuple4<String, Integer, Integer, Double> oldVal = maxList.get(newVal.f0);
                    if(oldVal.f3 < newVal.f3) {
                        maxList.remove(newVal.f0);
                        maxList.put(newVal.f0, newVal);
                    }
                } else {
                    maxList.put(newVal.f0, newVal);
                }
            }
            for(Map.Entry<String, Tuple4<String, Integer, Integer, Double>> item: maxList.entrySet()) {
                Tuple4<String, Integer, Integer, Double> maxTweet = item.getValue();
                collector.collect(new Tuple5<>(maxTweet.f0, context.window().getStart(), maxTweet.f1, maxTweet.f2, maxTweet.f3));
            }
        }
    }
}
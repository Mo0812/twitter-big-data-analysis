package de.moritzkanzler.comparison;

import de.moritzkanzler.filter.IgnoreDeletedPostsFilter;
import de.moritzkanzler.helper.ESClient;
import de.moritzkanzler.helper.ESConnection;
import de.moritzkanzler.helper.JobParameters;
import de.moritzkanzler.helper.TwitterConnection;
import de.moritzkanzler.keyBy.KeyByAllDistinct;
import de.moritzkanzler.keyBy.KeyByTweetPostId;
import de.moritzkanzler.map.RetweetLangFlatMap;
import de.moritzkanzler.map.TweetCountryCsvMap;
import de.moritzkanzler.map.TweetLangFlatMap;
import de.moritzkanzler.map.TwitterModelMap;
import de.moritzkanzler.model.TwitterPostData;
import de.moritzkanzler.sink.ESTweetRetweetShareCountry;
import de.moritzkanzler.streaming.TweetTopXRetweet;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
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
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static de.moritzkanzler.helper.JobType.BATCH;
import static de.moritzkanzler.helper.JobType.STREAM;

/**
 * COMPARISON 3: Share of retweets regarding to all tweets for each country
 */
public class TweetRetweetsShareCountry {

    /*
    Init Logger and Parameters
    */
    static Logger logger = LoggerFactory.getLogger(TweetTopXRetweet.class);
    static JobParameters paramTool = JobParameters.getInstance("src/main/resources/programs/tweetretweetssharecountry.properties");
    static String csvPath = paramTool.getProperty(BATCH, "out.csv.path", "");
    static Time windowSize = Time.minutes(15);
    static Time windowAllSize = Time.minutes(15);
    static Integer windowSequence = 15;
    static Integer decimalPlace = paramTool.getProperty(STREAM, "general.decimalplace", 0);

    /**
     * Process compare job
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // Establish twitter connection
        TwitterSource twitterSource = TwitterConnection.getTwitterConnection();

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
        COMPARISON 3
         */

        DataStream<Tuple8<String, Long, Integer, Integer, Double, Integer, Integer, Double>> tweetRetweetShareComparison = tweetRetweetShare
                .keyBy(new KeyByAllDistinct<>())
                .map(new RichMapFunction<Tuple5<String, Long, Integer, Integer, Double>, Tuple8<String, Long, Integer, Integer, Double, Integer, Integer, Double>>() {
                    private transient Map<String, Tuple4<String, Integer, Integer, Double>> countryMap;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        countryMap = new HashMap<>();

                        BufferedReader reader = Files.newBufferedReader(Paths.get(csvPath));
                        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
                        for (CSVRecord csvRecord : csvParser) {

                            String langCode = csvRecord.get(0);
                            String comparisonTimestamp = csvRecord.get(1);

                            Date cpDate = new Date(Long.parseLong(comparisonTimestamp));
                            Calendar calender = Calendar.getInstance();

                            calender.setTime(cpDate);
                            Integer cpHour = calender.get(Calendar.HOUR_OF_DAY);
                            Integer cpMinute = calender.get(Calendar.MINUTE);

                            countryMap.put(langCode + "-" + cpHour.toString() + cpMinute.toString(), new Tuple4<>(langCode, Integer.parseInt(csvRecord.get(2)), Integer.parseInt(csvRecord.get(3)), Double.parseDouble(csvRecord.get(4))));
                        }
                    }

                    @Override
                    public Tuple8<String, Long, Integer, Integer, Double, Integer, Integer, Double> map(Tuple5<String, Long, Integer, Integer, Double> retweet) throws Exception {
                        Date rtDate = new Date(retweet.f1);
                        Calendar calender = Calendar.getInstance();

                        calender.setTime(rtDate);
                        Integer rtHour = calender.get(Calendar.HOUR);
                        Integer rtMinute = calender.get(Calendar.MINUTE);
                        Integer diff = (int) rtMinute / windowSequence;
                        Integer diffedMinute = diff * windowSequence;

                        Tuple4<String, Integer, Integer, Double> batchValue = countryMap.get(retweet.f0 + "-" + rtHour.toString() + diffedMinute.toString());
                        if(batchValue != null) {
                            return new Tuple8<>(retweet.f0, retweet.f1, retweet.f2, retweet.f3, retweet.f4, batchValue.f1, batchValue.f2, batchValue.f3);
                        } else {
                            return new Tuple8<>(retweet.f0, retweet.f1, retweet.f2, retweet.f3, retweet.f4, 0, 0, 0d);
                        }
                    }
                });

        tweetRetweetShareComparison.print();

        env.execute("Twitter Tweet Retweet Share Country");
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
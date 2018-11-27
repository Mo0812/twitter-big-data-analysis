package de.moritzkanzler.comparison;

import de.moritzkanzler.exception.GeoCoordinatesNotFoundException;
import de.moritzkanzler.filter.IgnoreDeletedPostsFilter;
import de.moritzkanzler.helper.ESClient;
import de.moritzkanzler.helper.ESConnection;
import de.moritzkanzler.helper.JobParameters;
import de.moritzkanzler.helper.TwitterConnection;
import de.moritzkanzler.keyBy.KeyByAllDistinct;
import de.moritzkanzler.keyBy.KeyByTweetPostId;
import de.moritzkanzler.map.RetweetLangFlatMap;
import de.moritzkanzler.map.TweetCountryCsvMap;
import de.moritzkanzler.map.TwitterModelMap;
import de.moritzkanzler.model.TwitterPostData;
import de.moritzkanzler.sink.ESRetweetCountrySink;
import de.moritzkanzler.sink.ESRetweetPercentageCountrySink;
import de.moritzkanzler.streaming.TweetTopXRetweet;
import de.moritzkanzler.utils.GeoCoordinates;
import de.moritzkanzler.utils.Utils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.util.resources.cldr.aa.CalendarData_aa_ER;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static de.moritzkanzler.helper.JobType.BATCH;
import static de.moritzkanzler.helper.JobType.STREAM;

/**
 * COMPARISON 2
 */
public class TweetRetweetsCountry {

    /*
    Init Logger and Parameters
     */
    static Logger logger = LoggerFactory.getLogger(TweetTopXRetweet.class);
    static JobParameters paramTool = JobParameters.getInstance("src/main/resources/programs/tweetretweetscountry.properties");
    static String csvPath = paramTool.getProperty(BATCH, "percentage.out.csv.path", "");
    static Time windowSize = Time.seconds(15);
    static Time windowAllSize = Time.seconds(15);
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
         COMPARISON 2: Calculate percentage of retweets per country
        */
        DataStream<Tuple4<String, Long, Double, Integer>> retweetsLangPercentage = retweetsLangSum
                .keyBy(0)
                .timeWindowAll(windowAllSize)
                .process(new RetweetCountryPercentageProcessAll());

        DataStream<Tuple6<String, Long, Double, Integer, Double, Integer>> retweetsLangPercentageComparison = retweetsLangPercentage
                .keyBy(new KeyByAllDistinct<>())
                .flatMap(new RichFlatMapFunction<Tuple4<String, Long, Double, Integer>, Tuple6<String, Long, Double, Integer, Double, Integer>>() {
                    private transient Map<String, Tuple4<String, Long, Double, Integer>> countryMap;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        countryMap = new HashMap<>();

                        BufferedReader reader = Files.newBufferedReader(Paths.get(csvPath));
                        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
                        for(CSVRecord csvRecord: csvParser) {

                            String langCode = csvRecord.get(0);
                            String comparisonTimestamp = csvRecord.get(1);

                            Date cpDate = new Date(Long.parseLong(comparisonTimestamp));
                            Calendar calender = Calendar.getInstance();

                            calender.setTime(cpDate);
                            Integer cpHour = calender.get(Calendar.HOUR_OF_DAY);
                            Integer cpMinute = calender.get(Calendar.MINUTE);

                            countryMap.put(langCode + "-" + cpHour.toString() + cpMinute.toString(), new Tuple4<>(langCode, Long.parseLong(comparisonTimestamp), Double.parseDouble(csvRecord.get(2)), Integer.parseInt(csvRecord.get(3))));
                        }
                    }

                    @Override
                    public void flatMap(Tuple4<String, Long, Double, Integer> retweet, Collector<Tuple6<String, Long, Double, Integer, Double, Integer>> collector) throws Exception {
                        Date rtDate = new Date(retweet.f1);
                        Calendar calender = Calendar.getInstance();

                        calender.setTime(rtDate);
                        Integer rtHour = calender.get(Calendar.HOUR);
                        Integer rtMinute = calender.get(Calendar.MINUTE);
                        Integer diff = (int) rtMinute / windowSequence;
                        Integer diffedMinute = diff * windowSequence;

                        Tuple4<String, Long, Double, Integer> batchValue = countryMap.get(retweet.f0 + "-" + rtHour.toString() + diffedMinute.toString());
                        if(batchValue != null) {
                            collector.collect(new Tuple6<>(retweet.f0, retweet.f1, retweet.f2, retweet.f3, batchValue.f2, batchValue.f3));
                        } else {
                            collector.collect(new Tuple6<>(retweet.f0, retweet.f1, retweet.f2, retweet.f3, 0d, 0));
                        }
                    }
                });

        // ---
        retweetsLangPercentageComparison.print();

        env.execute("Twitter Retweets per Country comparison");
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

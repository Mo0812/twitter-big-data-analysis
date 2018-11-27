package de.moritzkanzler.batch;

import de.moritzkanzler.helper.JobParameters;
import de.moritzkanzler.map.TweetPerTimeMap;
import de.moritzkanzler.utils.Utils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

import static de.moritzkanzler.helper.JobType.BATCH;
import static de.moritzkanzler.helper.JobType.STREAM;

/**
 * METRIC 5: Share of retweets regarding to all tweets for each country
 */
public class TweetRetweetsShareCountry {

    /*
    Init Logger and Parameters
     */
    static Logger logger = LoggerFactory.getLogger(TweetRetweetsShareCountry.class);
    static JobParameters paramTool = JobParameters.getInstance("src/main/resources/programs/tweetretweetssharecountry.properties");
    static String csvInput = paramTool.getProperty(BATCH, "dump.csv.path", "");
    static String csvOutput = paramTool.getProperty(BATCH, "out.csv.path", "");
    static int windowSequence = 15;
    static Integer decimalPlace = paramTool.getProperty(STREAM, "general.decimalplace", 0);

    /**
     * Process batch job
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<Tuple4<Long, String, Long, Long>> tweets = env
                .readCsvFile(csvInput)
                .ignoreInvalidLines()
                .lineDelimiter(System.lineSeparator())
                .fieldDelimiter(",")
                .types(Long.class, String.class, Long.class, Long.class);

        DataSet<Tuple4<Long, String, Long, Long>> tweetsPerTime = tweets
                .map(new TweetPerTimeMap(windowSequence));

        DataSet<Tuple3<String, Long, Integer>> tweetsPerCountry = tweetsPerTime
                .flatMap(new TweetCountryFlatMap())
                .groupBy(0, 1)
                .sum(2);

        DataSet<Tuple3<String, Long, Integer>> retweetsPerCountry = tweetsPerTime
                .flatMap(new RetweetCountryFlatMap())
                .groupBy(0, 1)
                .sum(2);

        DataSet<Tuple5<String, Long, Integer, Integer, Double>> tweetShare = tweetsPerCountry
                .join(retweetsPerCountry)
                .where(0, 1)
                .equalTo(0, 1)
                .with(new TweetRetweetJoin());

        tweetShare.writeAsCsv(csvOutput, FileSystem.WriteMode.OVERWRITE);
        tweetShare.print();
    }

    /**
     * Flatmap function to count all tweets that will be recieved
     */
    public static class TweetCountryFlatMap implements FlatMapFunction<Tuple4<Long, String, Long, Long>, Tuple3<String, Long, Integer>> {
        @Override
        public void flatMap(Tuple4<Long, String, Long, Long> tweet, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
            collector.collect(new Tuple3<>(tweet.f1, tweet.f2, 1));
        }
    }

    /**
     * Flatmap function to count all retweets that will be recieved
     */
    public static class RetweetCountryFlatMap implements FlatMapFunction<Tuple4<Long, String, Long, Long>, Tuple3<String, Long, Integer>> {
        @Override
        public void flatMap(Tuple4<Long, String, Long, Long> tweet, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
            if(tweet.f3 != -1) {
                collector.collect(new Tuple3<>(tweet.f1, tweet.f2, 1));
            }
        }
    }

    /**
     * Join function to calculate the share of the retweets against the tweets according to the recieved language code
     */
    public static class TweetRetweetJoin implements JoinFunction<Tuple3<String, Long, Integer>, Tuple3<String, Long, Integer>, Tuple5<String, Long, Integer, Integer, Double>> {
        @Override
        public Tuple5<String, Long, Integer, Integer, Double> join(Tuple3<String, Long, Integer> tweet, Tuple3<String, Long, Integer> retweet) throws Exception {
            String lang = retweet.f0;
            Double percentage = (double)  retweet.f2 / tweet.f2;
            return new Tuple5<>(lang, tweet.f1, retweet.f2, tweet.f2, percentage);
        }
    }
}

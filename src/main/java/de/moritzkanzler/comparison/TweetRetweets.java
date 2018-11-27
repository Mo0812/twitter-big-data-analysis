/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.moritzkanzler.comparison;

import de.moritzkanzler.filter.IgnoreDeletedPostsFilter;
import de.moritzkanzler.helper.ESClient;
import de.moritzkanzler.helper.ESConnection;
import de.moritzkanzler.helper.JobParameters;
import de.moritzkanzler.helper.TwitterConnection;
import de.moritzkanzler.keyBy.KeyByAllDistinct;
import de.moritzkanzler.map.TweetCsvMap;
import de.moritzkanzler.map.TwitterModelMap;
import de.moritzkanzler.model.TwitterPostData;
import de.moritzkanzler.sink.ESRetweetSink;
import de.moritzkanzler.streaming.TweetTopXRetweet;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */

/**
 * COMPARISON 1: Retweets in total per time
 */
public class TweetRetweets {

    /*
    Init Logger and Parameters
     */
    static Logger logger = LoggerFactory.getLogger(TweetTopXRetweet.class);
    static JobParameters paramTool = JobParameters.getInstance("src/main/resources/programs/tweetretweets.properties");
    static String csvPath = paramTool.getProperty(BATCH, "out.csv.path", "");
    static Time windowSize = Time.seconds(15);
    static Integer windowSequence = 15;

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

        DataStream<String> streamSource = env.addSource(twitterSource);

        DataStream<TwitterPostData> tweets = streamSource
                .filter(new IgnoreDeletedPostsFilter())
                .map(new TwitterModelMap());

        DataStream<Tuple2<Long, Integer>> retweetMap = tweets
                .flatMap(new TweetRetweetFlatMap());


        DataStream<Tuple3<Long, Long, Integer>> retweetSum = retweetMap
                .keyBy(0)
                .timeWindow(windowSize)
                .process(new SumTweetProcess());

        DataStream<Tuple2<Long, Integer>> retweetTime = retweetSum
                .map(new CondenseInformationFlatMap())
                .keyBy(0)
                .timeWindowAll(windowSize)
                .sum(1);

        DataStream<Tuple3<Long, Integer, Integer>> retweetTimeComparison = retweetTime
                .keyBy(new KeyByAllDistinct<>())
                .map(new RichMapFunction<Tuple2<Long, Integer>, Tuple3<Long, Integer, Integer>>() {
                    private transient Map<String, Tuple2<String, Integer>> timeMap;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        timeMap = new HashMap<>();

                        BufferedReader reader = Files.newBufferedReader(Paths.get(csvPath));
                        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
                        for (CSVRecord csvRecord : csvParser) {

                            String comparisonTimestamp = csvRecord.get(0);

                            Date cpDate = new Date(Long.parseLong(comparisonTimestamp));
                            Calendar calender = Calendar.getInstance();

                            calender.setTime(cpDate);
                            Integer cpHour = calender.get(Calendar.HOUR_OF_DAY);
                            Integer cpMinute = calender.get(Calendar.MINUTE);

                            timeMap.put(cpHour.toString() + cpMinute.toString(), new Tuple2<>(comparisonTimestamp, Integer.parseInt(csvRecord.get(1))));
                        }
                    }

                    @Override
                    public Tuple3<Long, Integer, Integer> map(Tuple2<Long, Integer> retweet) throws Exception {
                        Date rtDate = new Date(retweet.f0);
                        Calendar calender = Calendar.getInstance();

                        calender.setTime(rtDate);
                        Integer rtHour = calender.get(Calendar.HOUR);
                        Integer rtMinute = calender.get(Calendar.MINUTE);
                        Integer diff = (int) rtMinute / windowSequence;
                        Integer diffedMinute = diff * windowSequence;

                        Tuple2<String, Integer> batchValue = timeMap.get(rtHour.toString() + diffedMinute.toString());
                        if(batchValue != null) {
                            return new Tuple3<>(retweet.f0, retweet.f1, batchValue.f1);
                        } else {
                            return new Tuple3<>(retweet.f0, retweet.f1, 0);
                        }
                    }
                });

        retweetTimeComparison.print();

        env.execute("Twitter retweet count comparison");
	}

    /**
     * Flatmap function to extract valid retweets and there id to sum them later
     */
    public static class TweetRetweetFlatMap implements FlatMapFunction<TwitterPostData, Tuple2<Long, Integer>> {
        @Override
        public void flatMap(TwitterPostData twitterPostData, Collector<Tuple2<Long, Integer>> collector) throws Exception {
            if(twitterPostData.hasRetweet()) {
                collector.collect(new Tuple2<>(twitterPostData.getRetweetTwitterPostData().getId(), 1));
            }
        }
    }

    /**
     * Process function to sum retweets and add timestamp information
     */
    public static class SumTweetProcess extends ProcessWindowFunction<Tuple2<Long, Integer>, Tuple3<Long, Long, Integer>, Tuple, TimeWindow> {
        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2<Long, Integer>> iterable, Collector<Tuple3<Long, Long, Integer>> collector) throws Exception {
            int sum = 0;
            long id = 0L;
            for(Tuple2<Long, Integer> item: iterable) {
                id = item.f0;
                sum += item.f1;
            }
            collector.collect(new Tuple3<>(id, context.window().getStart(), sum));
        }
    }

    /**
     * Map function to delete count value
     */
    public static class CondenseInformationFlatMap implements MapFunction<Tuple3<Long, Long, Integer>, Tuple2<Long, Integer>> {
        @Override
        public Tuple2<Long, Integer> map(Tuple3<Long, Long, Integer> in) throws Exception {
            return new Tuple2<>(in.f1, in.f2);
        }
    }
}

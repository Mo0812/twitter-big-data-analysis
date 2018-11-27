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

package de.moritzkanzler.streaming;

import de.moritzkanzler.filter.IgnoreDeletedPostsFilter;
import de.moritzkanzler.helper.ESClient;
import de.moritzkanzler.helper.ESConnection;
import de.moritzkanzler.helper.JobParameters;
import de.moritzkanzler.helper.TwitterConnection;
import de.moritzkanzler.keyBy.KeyByAllDistinct;
import de.moritzkanzler.keyBy.KeyByTweetPostId;
import de.moritzkanzler.model.TwitterPostData;
import de.moritzkanzler.map.TwitterModelMap;
import de.moritzkanzler.sink.ESRetweetSink;
import de.moritzkanzler.sink.ESTopRetweetSink;
import de.moritzkanzler.utils.Utils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
 * METRIC 2: Top X Retweets per time
 */
public class TweetTopXRetweet {

    /*
    Init Logger and Parameters
    */
    static Logger logger = LoggerFactory.getLogger(TweetTopXRetweet.class);
    static JobParameters paramTool = JobParameters.getInstance("src/main/resources/programs/tweettopxretweet.properties");
    static String indexName = paramTool.getProperty(STREAM, "elasticsearch.index", "");
    static String typeName = paramTool.getProperty(STREAM, "elasticsearch.type", "");
    static boolean deleteIndex = paramTool.getProperty(STREAM, "elasticsearch.delete", false);
    static boolean esOutput = paramTool.getProperty(STREAM, "elasticsearch.output", false);
    static boolean csvOutput = paramTool.getProperty(STREAM, "csv.output", false);
    static String csvDumpPath = paramTool.getProperty(STREAM, "dump.csv.path", "");
    static String csvOutputPath = paramTool.getProperty(STREAM, "out.csv.path", "");
    static Integer topRetweetsMax = paramTool.getProperty(STREAM, "top.x", 5);
    static Time windowSize = Time.minutes(1);
    static Time windowAllSize = Time.minutes(1);

    /**
     * Process stream job
     * @param args
     * @throws Exception
     */
	public static void main(String[] args) throws Exception {
		// Establish twitter connection
        TwitterSource twitterSource = TwitterConnection.getTwitterConnection();

        if(esOutput) {
            prepareES();
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> streamSource = env.addSource(twitterSource);

        DataStream<TwitterPostData> tweets = streamSource
                .filter(new IgnoreDeletedPostsFilter())
                .map(new TwitterModelMap());

        DataStream<Tuple2<TwitterPostData, Integer>> retweetMap = tweets
                .keyBy(new KeyByTweetPostId())
                .flatMap(new RetweetFlatMap());


        DataStream<Tuple2<TwitterPostData, Integer>> retweetSum = retweetMap
                .keyBy(new KeyByRetweetPostId())
                .timeWindow(windowSize)
                .sum(1);

        DataStream<Tuple5<Long, String, String, Long, Integer>> topXTweets = retweetSum
                .keyBy(new KeyByAllDistinct<Tuple2<TwitterPostData, Integer>>())
                .timeWindowAll(windowAllSize)
                .process(new TopXRetweetsProcessAll());

        /*
        Elasticsearch output
         */
        if(esOutput) {
            ElasticsearchSink<Tuple5<Long, String, String, Long, Integer>> esSink = ESConnection.buildESSink(new ESTopRetweetSink(logger, indexName, typeName));
            topXTweets.addSink(esSink);
        }

        /*
        CSV output
         */
        if(csvOutput) {
            DataStream<Tuple5<Long, Long, Long, Long, String>> tweetsCsv = tweets
                    .map(new RetweetCsvMap());
            tweetsCsv.writeAsCsv(csvDumpPath, FileSystem.WriteMode.OVERWRITE);
            topXTweets.writeAsCsv(csvOutputPath, FileSystem.WriteMode.OVERWRITE);
        } else {
            topXTweets.writeAsCsv(csvOutputPath, FileSystem.WriteMode.OVERWRITE);
            topXTweets.print();
        }

        env.execute("Twitter example");
	}

    /**
     * Prepration of ES index to save data later on
     * @throws Exception
     */
	private static void prepareES() throws Exception {

        //Prepare ES Index
        XContentBuilder esProperties = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("properties")
                    .startObject("tId")
                        .field("type", "long")
                    .endObject()
                    .startObject("screenname")
                        .field("type", "keyword")
                    .endObject()
                    .startObject("tweet")
                        .field("type", "text")
                    .endObject()
                    .startObject("watermark")
                        .field("type", "date")
                    .endObject()
                    .startObject("cnt")
                        .field("type", "integer")
                    .endObject()
                .endObject()
            .endObject();

        // Manage Index in ES
        ESClient esClient = ESClient.getInstance();
        esClient.buildIndex(indexName, true);
        esClient.buildMapping(indexName, typeName, esProperties);
    }

    /**
     * Flatmap function for process only retweets and for later counting them
     */
    public static class RetweetFlatMap implements FlatMapFunction<TwitterPostData, Tuple2<TwitterPostData, Integer>> {
        @Override
        public void flatMap(TwitterPostData twitterPostData, Collector<Tuple2<TwitterPostData, Integer>> collector) throws Exception {
            if(twitterPostData.hasRetweet()) {
                TwitterPostData retweet = twitterPostData.getRetweetTwitterPostData();
                collector.collect(new Tuple2<>(retweet, 1));
            }
        }
    }

    /**
     * KeySelector after tweet ID
     */
    public static class KeyByRetweetPostId implements KeySelector<Tuple2<TwitterPostData, Integer>, Long> {
        @Override
        public Long getKey(Tuple2<TwitterPostData, Integer> tweet) throws Exception {
            return tweet.f0.getId();
        }
    }

    /**
     * Processing of stream to prepare it for saving it as csv file
     */
    public static class RetweetCsvMap implements MapFunction<TwitterPostData, Tuple5<Long, Long, Long, Long, String>> {
        @Override
        public Tuple5<Long, Long, Long, Long, String> map(TwitterPostData tweet) throws Exception {
            TwitterPostData retweet = tweet.hasRetweet() ? tweet.getRetweetTwitterPostData() : null;
            Long rtId = retweet != null ? retweet.getId() : 0L;
            Long rtWatermark = retweet != null ? retweet.getWatermark() : 0L;
            String rtUser = retweet != null && retweet.hasUser() ? retweet.getUser().getScreen_name() : "-";

            return new Tuple5<>(tweet.getId(), tweet.getWatermark(), rtId, rtWatermark, Utils.escape(rtUser));
        }
    }

    /**
     * Process function to get the X most successfull tweets (tweets which are retweeted most) in the given time window
     */
    public static class TopXRetweetsProcessAll extends ProcessAllWindowFunction<Tuple2<TwitterPostData, Integer>, Tuple5<Long, String, String, Long, Integer>, TimeWindow> {
        @Override
        public void process(Context context, Iterable<Tuple2<TwitterPostData, Integer>> iterable, Collector<Tuple5<Long, String, String, Long, Integer>> collector) throws Exception {
            List<Tuple2<TwitterPostData, Integer>> topRetweets = new ArrayList<>();
            for(Tuple2<TwitterPostData, Integer> retweet: iterable) {
                if(topRetweets.size() < topRetweetsMax) {
                    topRetweets.add(retweet);
                } else {
                    List<Tuple2<TwitterPostData, Integer>> forRemoval = topRetweets
                            .stream()
                            .filter(rt -> rt.f1 < retweet.f1)
                            .collect(Collectors.toList());
                    if(forRemoval.size() > 0) {
                        topRetweets.remove(forRemoval.get(0));
                        topRetweets.add(retweet);
                    }
                }
            }
            topRetweets.forEach(retweet -> {
                if(retweet.f0.hasUser()) {
                    Long time = context.window().getStart();
                    collector.collect(new Tuple5<>(retweet.f0.getId(), retweet.f0.getUser().getScreen_name(), retweet.f0.getText(), time, retweet.f1));
                }
            });
        }
    }
}

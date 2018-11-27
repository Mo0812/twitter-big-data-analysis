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

import com.sun.org.apache.xpath.internal.operations.Bool;
import de.moritzkanzler.filter.IgnoreDeletedPostsFilter;
import de.moritzkanzler.helper.ESClient;
import de.moritzkanzler.helper.ESConnection;
import de.moritzkanzler.helper.JobParameters;
import de.moritzkanzler.helper.TwitterConnection;
import de.moritzkanzler.keyBy.KeyByAllDistinct;
import de.moritzkanzler.keyBy.KeyByTweetPostId;
import de.moritzkanzler.map.TweetCsvMap;
import de.moritzkanzler.map.TwitterModelMap;
import de.moritzkanzler.model.TwitterPostData;
import de.moritzkanzler.sink.ESRetweetSink;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
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
import scala.Int;

import javax.annotation.Nullable;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

import static de.moritzkanzler.helper.JobType.PREDICTION;
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
 * METRIC 1: Retweets in total per time
 */
public class TweetRetweets {

    /*
    Init Logger and Parameters
     */
    static Logger logger = LoggerFactory.getLogger(TweetTopXRetweet.class);
    static JobParameters paramTool = JobParameters.getInstance("src/main/resources/programs/tweetretweets.properties");
    static String indexName = paramTool.getProperty(STREAM, "elasticsearch.index", "");
    static String typeName = paramTool.getProperty(STREAM, "elasticsearch.type", "");
    static boolean deleteIndex = paramTool.getProperty(STREAM, "elasticsearch.delete", false);
    static boolean esOutput = paramTool.getProperty(STREAM, "elasticsearch.output", false);
    static boolean csvOutput = paramTool.getProperty(STREAM, "csv.output", false);
    static String csvDumpPath = paramTool.getProperty(STREAM, "dump.csv.path", "");
    static String csvOutputPath = paramTool.getProperty(STREAM, "out.csv.path", "");
    static Time windowSize = Time.minutes(15);

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
        /*
        Elasticsearch output
         */
        if(esOutput) {
            ElasticsearchSink<Tuple2<Long, Integer>> esSink = ESConnection.buildESSink(new ESRetweetSink(logger, indexName, typeName));
            retweetTime.addSink(esSink);
        }
        /*
        CSV output
         */
        if(csvOutput) {
            DataStream<Tuple3<Long, Long, Long>> tweetsCsv = tweets
                    .map(new TweetCsvMap());
            tweetsCsv.writeAsCsv(csvDumpPath, FileSystem.WriteMode.OVERWRITE);

            retweetTime.writeAsCsv(csvOutputPath, FileSystem.WriteMode.OVERWRITE);
        } else {
            retweetTime.print();
        }

        env.execute("Twitter retweet count");
	}

    /**
     * Prepration of ES index to save data later on
     * @throws Exception
     */
	private static void prepareES(String indexName, String typeName, boolean delete) throws Exception {

        //Prepare ES Index
        XContentBuilder esProperties = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("properties")
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
        esClient.buildIndex(indexName, delete);
        esClient.buildMapping(indexName, typeName, esProperties);
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

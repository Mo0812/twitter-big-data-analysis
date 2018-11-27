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

package de.moritzkanzler.prediction;

import de.moritzkanzler.filter.IgnoreDeletedPostsFilter;
import de.moritzkanzler.helper.ESClient;
import de.moritzkanzler.helper.ESConnection;
import de.moritzkanzler.helper.JobParameters;
import de.moritzkanzler.helper.TwitterConnection;
import de.moritzkanzler.keyBy.KeyByAllDistinct;
import de.moritzkanzler.map.TwitterModelMap;
import de.moritzkanzler.model.TwitterPostData;
import de.moritzkanzler.sink.ESRetweetPredictionSink;
import de.moritzkanzler.streaming.TweetTopXRetweet;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
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

import java.util.ArrayList;
import java.util.List;

import static de.moritzkanzler.helper.JobType.PREDICTION;

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
 * PROGRESSION 1: Average of last 5 values
 * PROGRESSION 2: Simpleregression
 * PROGRESSION 3: Reinforcement learning algorithm for tracking non-stationary reward data
 */
public class TweetRetweets {

    /*
    Init Logger and Parameters
     */
    static Logger logger = LoggerFactory.getLogger(TweetTopXRetweet.class);
    static JobParameters paramTool = JobParameters.getInstance("src/main/resources/programs/tweetretweets.properties");
    static String indexName = paramTool.getProperty(PREDICTION, "elasticsearch.index", "");
    static String typeName = paramTool.getProperty(PREDICTION, "elasticsearch.type", "");
    static boolean deleteIndex = paramTool.getProperty(PREDICTION, "elasticsearch.delete", false);
    static boolean esOutput = paramTool.getProperty(PREDICTION, "elasticsearch.output", false);
    static boolean csvOutput = paramTool.getProperty(PREDICTION, "csv.output", false);
    static String csvOutputPathAvg = paramTool.getProperty(PREDICTION, "avg.out.csv.path", "");
    static String csvOutputPathRegr = paramTool.getProperty(PREDICTION, "regr.out.csv.path", "");
    static String csvOutputPathRl = paramTool.getProperty(PREDICTION, "rl.out.csv.path", "");
    static Integer maxHistory = paramTool.getProperty(PREDICTION, "history.max", 5);
    static Time windowSize = Time.minutes(1);

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
        PREDICTION 1
         */
        DataStream<Tuple5<String, Long, Integer, Double, Double>> retweetTimePrediction = retweetTime
            .keyBy(new KeyByAllDistinct<Tuple2<Long, Integer>>())
            .process(new PredictionProcess());

        /*
        PREDICTION 2
         */
        DataStream<Tuple5<String, Long, Integer, Double, Double>> retweetTimePredictionRegression = retweetTime
                .keyBy(new KeyByAllDistinct<Tuple2<Long, Integer>>())
                .process(new PredictionRegressionProcess());

        /*
        PREDICTION 3
         */
        DataStream<Tuple5<String, Long, Integer, Double, Double>> retweetTimePredictionRl = retweetTime
                .keyBy(new KeyByAllDistinct<Tuple2<Long, Integer>>())
                .process(new PredictionReinforcementLearning());

        /*
        Elasticsearch output
         */
        if(esOutput) {
            ElasticsearchSink<Tuple5<String, Long, Integer, Double, Double>> esSinkAvg = ESConnection.buildESSink(new ESRetweetPredictionSink(logger, indexName, typeName));
            retweetTimePrediction.addSink(esSinkAvg);

            ElasticsearchSink<Tuple5<String, Long, Integer, Double, Double>> esSinkRegr = ESConnection.buildESSink(new ESRetweetPredictionSink(logger, indexName, typeName));
            retweetTimePredictionRegression.addSink(esSinkRegr);

            ElasticsearchSink<Tuple5<String, Long, Integer, Double, Double>> esSinkRl = ESConnection.buildESSink(new ESRetweetPredictionSink(logger, indexName, typeName));
            retweetTimePredictionRl.addSink(esSinkRl);
        }
        /*
        CSV output
         */
        if(csvOutput) {
            retweetTimePrediction
                    .map(new RemoveTypeMap())
                    .writeAsCsv(csvOutputPathAvg, FileSystem.WriteMode.OVERWRITE);
            retweetTimePredictionRegression
                    .map(new RemoveTypeMap())
                    .writeAsCsv(csvOutputPathRegr, FileSystem.WriteMode.OVERWRITE);
            retweetTimePredictionRl
                    .map(new RemoveTypeMap())
                    .writeAsCsv(csvOutputPathRl, FileSystem.WriteMode.OVERWRITE);
        } else {
            retweetTimePrediction.print();
            retweetTimePredictionRegression.print();
            retweetTimePredictionRl.print();
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
                    .startObject("prediction_type")
                        .field("type", "keyword")
                    .endObject()
                    .startObject("watermark")
                        .field("type", "date")
                    .endObject()
                    .startObject("cnt")
                        .field("type", "integer")
                    .endObject()
                    .startObject("prediction")
                        .field("type", "double")
                    .endObject()
                    .startObject("deviation")
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
     * Flatmap function for getting every tweet with a retweet and its count
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
     * Process function to sum all of the same retweetet tweets together in a given time window
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
     * Removing the tweet ID information
     */
    public static class CondenseInformationFlatMap implements MapFunction<Tuple3<Long, Long, Integer>, Tuple2<Long, Integer>> {
        @Override
        public Tuple2<Long, Integer> map(Tuple3<Long, Long, Integer> in) throws Exception {
            return new Tuple2<>(in.f1, in.f2);
        }
    }

    /**
     * Flatmap function for removing the prediciton type identifier in the csv output
     */
    public static class RemoveTypeMap implements MapFunction<Tuple5<String, Long, Integer, Double, Double>, Tuple4<Long, Integer, Double, Double>> {
        @Override
        public Tuple4<Long, Integer, Double, Double> map(Tuple5<String, Long, Integer, Double, Double> in) throws Exception {
            return new Tuple4<>(in.f1, in.f2, in.f3, in.f4);
        }
    }

    /**
     * Process function for PREDICTION 1
     */
    public static class PredictionProcess extends ProcessFunction<Tuple2<Long, Integer>, Tuple5<String, Long, Integer, Double, Double>> {
        private transient ListState<Integer> history;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<Integer> descriptor = new ListStateDescriptor<Integer>(
                    "history",
                    Integer.class
            );
            history = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(Tuple2<Long, Integer> in, Context context, Collector<Tuple5<String, Long, Integer, Double, Double>> collector) throws Exception {
            if(history == null) {
                List<Integer> list = new ArrayList<>();
                history.update(list);
            }

            List<Integer> currentList = Lists.newArrayList(history.get());

            Double avgHistory = 0.0;
            if(currentList.size() > 0) {
                int divider = currentList.size();
                if(maxHistory <= currentList.size()) {
                    divider = maxHistory;
                }
                Integer sumHistory = 0;
                int cLIndex = currentList.size() - 1;
                for(int i = cLIndex; i > cLIndex - divider ; i--) {
                    Integer val = currentList.get(i);
                    sumHistory += val;
                }
                avgHistory = (double) sumHistory / divider;
            }

            if(currentList.size() > maxHistory) {
                List<Integer> subList = currentList.subList(0, currentList.size() - maxHistory);
                subList.clear();
            }

            currentList.add(in.f1);
            history.update(currentList);

            collector.collect(new Tuple5<>("AVG", in.f0, in.f1, avgHistory, in.f1 - avgHistory));

        }
    }

    /**
     * Process function for PREDICTION 2
     */
    public static class PredictionRegressionProcess extends ProcessFunction<Tuple2<Long, Integer>, Tuple5<String, Long, Integer, Double, Double>> {
        private transient ValueState<SimpleRegression> regressionModel;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<SimpleRegression> descriptor = new ValueStateDescriptor<>(
                    "regressionModel",
                    SimpleRegression.class
            );
            regressionModel = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Tuple2<Long, Integer> in, Context context, Collector<Tuple5<String, Long, Integer, Double, Double>> collector) throws Exception {
            SimpleRegression model = regressionModel.value();
            if(model == null) {
                model = new SimpleRegression(false);
            }

            Double prediction = model.predict(in.f0);
            if(Double.isNaN(prediction)) {
                prediction = 0.0;
            }

            model.addData(in.f0, in.f1);
            regressionModel.update(model);

            collector.collect(new Tuple5<>("SIMPLEREGRESSION", in.f0, in.f1, prediction, in.f1 - prediction));

        }
    }

    /**
     * Process function for PREDICTION 3
     */
    public static class PredictionReinforcementLearning extends ProcessFunction<Tuple2<Long, Integer>, Tuple5<String, Long, Integer, Double, Double>> {
        private transient ValueState<Double> lastValue;
        private transient ValueState<Integer> lastReward;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Double> descriptorValue = new ValueStateDescriptor<>(
                    "lastValue",
                    Double.class
            );
            ValueStateDescriptor<Integer> descriptorReward = new ValueStateDescriptor<>(
                    "lastReward",
                    Integer.class
            );

            lastValue = getRuntimeContext().getState(descriptorValue);
            lastReward = getRuntimeContext().getState(descriptorReward);
        }

        @Override
        public void processElement(Tuple2<Long, Integer> in, Context context, Collector<Tuple5<String, Long, Integer, Double, Double>> collector) throws Exception {
            Double lastVal = lastValue.value();
            Integer lastRew = lastReward.value();
            if(lastVal == null) {
                lastVal = 0d;
            }
            if(lastRew == null) {
                lastRew = 0;
            }

            Double newVal = lastVal + 0.1 * (lastRew - lastVal);
            int newRew = in.f1;

            collector.collect(new Tuple5<>("REINFORCEMENT", in.f0, in.f1, newVal, in.f1 - newVal));

            lastValue.update(newVal);
            lastReward.update(newRew);
        }
    }
}

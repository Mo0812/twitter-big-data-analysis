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

package de.moritzkanzler.batch;

import de.moritzkanzler.helper.JobParameters;
import de.moritzkanzler.map.TweetPerTime3Map;
import de.moritzkanzler.map.TweetPerTimeMap;
import de.moritzkanzler.streaming.TweetTopXRetweet;
import de.moritzkanzler.utils.Utils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static de.moritzkanzler.helper.JobType.BATCH;
import static de.moritzkanzler.helper.JobType.STREAM;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application i qnto a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
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
    static String csvInput = paramTool.getProperty(BATCH, "dump.csv.path", "");
    static String csvOutput = paramTool.getProperty(BATCH, "out.csv.path", "");
    static int windowSequence = 15;

    /**
     * Process batch job
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // set up the batch execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSet<Tuple3<Long, Long, Long>> tweets = env
                .readCsvFile(csvInput)
                .ignoreInvalidLines()
                .lineDelimiter(System.lineSeparator())
                .fieldDelimiter(",")
                .types(Long.class, Long.class, Long.class);

        DataSet<Tuple3<Long, Long, Long>> tweetsPerTime = tweets
                .map(new TweetPerTime3Map(windowSequence));

        DataSet<Tuple2<Long, Integer>> retweetsPerTime = tweetsPerTime
                .filter(new IgnoreNonRetweetsFilter())
                .map(new SumRetweetsMap())
                .groupBy(0)
                .sum(1);

        retweetsPerTime.writeAsCsv(csvOutput, FileSystem.WriteMode.OVERWRITE);
        retweetsPerTime.print();

        // execute program
        //env.execute("Flink Batch Java API Skeleton");
    }

    /**
     * Check if a given tweet retweet an other tweet
     */
    static class IgnoreNonRetweetsFilter implements FilterFunction<Tuple3<Long, Long, Long>> {
        @Override
        public boolean filter(Tuple3<Long, Long, Long> in) throws Exception {
            return in.f2 != -1L;
        }
    }

    /**
     * Map function for counting retweets
     */
    static class SumRetweetsMap implements MapFunction<Tuple3<Long, Long, Long>, Tuple2<Long, Integer>> {
        @Override
        public Tuple2<Long, Integer> map(Tuple3<Long, Long, Long> in) throws Exception {
            return new Tuple2<>(in.f1, 1);
        }
    }
}
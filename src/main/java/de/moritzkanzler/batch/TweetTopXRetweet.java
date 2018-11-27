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
import de.moritzkanzler.map.TweetPerTime5Map;
import de.moritzkanzler.model.TwitterPostData;
import de.moritzkanzler.utils.Utils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
 * METRIC 2: Top X Retweets per time
 */
public class TweetTopXRetweet {

	/*
    Init Logger and Parameters
     */
	static Logger logger = LoggerFactory.getLogger(TweetTopXRetweet.class);
	static JobParameters paramTool = JobParameters.getInstance("src/main/resources/programs/tweettopxretweet.properties");
	static String csvInput = paramTool.getProperty(BATCH, "dump.csv.path", "");
	static String csvOutput = paramTool.getProperty(BATCH, "out.csv.path", "");
	static Integer topRetweetsMax = paramTool.getProperty(STREAM, "top.x", 5);
	static int windowSequence = 1;

	/**
	 * Process batch job
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataSet<Tuple5<Long, Long, Long, Long, String>> tweets = env
				.readCsvFile(csvInput)
				.ignoreInvalidLines()
				.lineDelimiter(System.lineSeparator())
				.fieldDelimiter(",")
				.types(Long.class, Long.class, Long.class, Long.class, String.class);

		DataSet<Tuple5<Long, Long, Long, Long, String>> retweetsPerTime = tweets
				.filter(new RetweetFilter())
				.map(new TweetPerTime5Map(windowSequence));

		DataSet<Tuple5<Long, Long, Long, String, Integer>> topXRetweets = retweetsPerTime
				.map(new RetweetSumMap())
				.groupBy(1, 2)
				.sum(4)
				.groupBy(1, 2)
				.reduceGroup(new TopXRetweetsGroupReduce());


		topXRetweets.writeAsCsv(csvOutput, FileSystem.WriteMode.OVERWRITE);
		topXRetweets.print();

		// execute program
		//env.execute("Flink Batch Java API Skeleton");
	}

	/**
	 * Filter function for filtering all tweets that do not reference to retweets
	 */
	public static class RetweetFilter implements FilterFunction<Tuple5<Long, Long, Long, Long, String>> {
		@Override
		public boolean filter(Tuple5<Long, Long, Long, Long, String> tweet) throws Exception {
			return tweet.f2 != 0;
		}
	}

	/**
	 * Map function to prepare a sum over the data
	 */
	public static class RetweetSumMap implements MapFunction<Tuple5<Long, Long, Long, Long, String>, Tuple5<Long, Long, Long, String, Integer>> {
		@Override
		public Tuple5<Long, Long, Long, String, Integer> map(Tuple5<Long, Long, Long, Long, String> in) throws Exception {
			return new Tuple5<>(in.f0, in.f1, in.f2, in.f4, 1);
		}
	}

	/**
	 * Group reduce for determine the top X retweets in a given time window
	 */
	public static class TopXRetweetsGroupReduce implements GroupReduceFunction<Tuple5<Long, Long, Long, String, Integer>, Tuple5<Long, Long, Long, String, Integer>> {
		@Override
		public void reduce(Iterable<Tuple5<Long, Long, Long, String, Integer>> iterable, Collector<Tuple5<Long, Long, Long, String, Integer>> collector) throws Exception {
			List<Tuple5<Long, Long, Long, String, Integer>> topXRetweets = new ArrayList<>();
			for(Tuple5<Long, Long, Long, String, Integer> retweet: iterable) {
				if(topXRetweets.size() < topRetweetsMax) {
					topXRetweets.add(retweet);
				} else {
					List<Tuple5<Long, Long, Long, String, Integer>> forRemoval = topXRetweets
							.stream()
							.filter(rt -> rt.f4 < retweet.f4)
							.collect(Collectors.toList());
					if(forRemoval.size() > 0) {
						topXRetweets.remove(forRemoval.get(0));
						topXRetweets.add(retweet);
					}
				}
			}
			topXRetweets.forEach(retweet -> {
				collector.collect(retweet);
			});
		}
	}
}

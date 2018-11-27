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

import de.moritzkanzler.exception.GeoCoordinatesNotFoundException;
import de.moritzkanzler.helper.JobParameters;
import de.moritzkanzler.map.TweetPerTimeMap;
import de.moritzkanzler.utils.GeoCoordinates;
import de.moritzkanzler.utils.Utils;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weka.classifiers.*;
import weka.classifiers.functions.LinearRegression;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static de.moritzkanzler.helper.JobType.BATCH;
import static de.moritzkanzler.helper.JobType.STREAM;

/**
 * Skeleton for a Flink Batch Job.
 *
 * <p>For a tutorial how to write a Flink batch application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution,
 * change the main class in the POM.xml file to this class (simply search for 'mainClass')
 * and run 'mvn clean package' on the command line.
 */

/**
 * METRIC 3 & 4
 */
public class TweetRetweetsCountry {

	/*
    Init Logger and Parameters
     */
	static Logger logger = LoggerFactory.getLogger(TweetRetweetsShareCountry.class);
	static JobParameters paramTool = JobParameters.getInstance("src/main/resources/programs/tweetretweetscountry.properties");
	static String csvInput = paramTool.getProperty(BATCH, "dump.csv.path", "");
	static boolean csvDeleteLastLine = paramTool.getProperty(BATCH, "csv.removelastline", false);
	static String csvGeoOutput = paramTool.getProperty(BATCH, "geo.out.csv.path", "");
	static String csvPercentageOutput = paramTool.getProperty(BATCH, "percentage.out.csv.path", "");
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

		/*
		Metric 3: Retweets per country per time
		Coming from the tweets adjust to a given time window, here the retweets get filtered at first. After that the coordinates of the given language code will be added through a flatmap function
		 */
		DataSet<Tuple5<String, Double, Double, Long, Integer>> retweetsLangGeo = tweetsPerTime
				.filter(new RetweetFilter())
				.flatMap(new RetweetGeoFlatMap())
				.groupBy(0, 3)
				.sum(4);

		/*
		Metric 4: Percentage of all retweets per country
		In this part we also begin with the same stream collection as above, but calculate the share of retweets per country over all retweets collected per time frame
		 */
		DataSet<Tuple4<String, Long, Double, Integer>> retweetsLangPercentage = tweetsPerTime
				.filter(new RetweetFilter())
				.map(new RetweetCountMap())
				.groupBy(0, 1)
				.sum(2)
				.groupBy(1)
				.reduceGroup(new RetweetPercentageGroupReduce());

		retweetsLangGeo.writeAsCsv(csvGeoOutput, FileSystem.WriteMode.OVERWRITE);
		retweetsLangGeo.print();

		retweetsLangPercentage.writeAsCsv(csvPercentageOutput, FileSystem.WriteMode.OVERWRITE);
		retweetsLangPercentage.print();
	}

	/**
	 * Check if a given tweet retweet an other tweet
	 */
	public static class RetweetFilter implements FilterFunction<Tuple4<Long, String, Long, Long>> {
		@Override
		public boolean filter(Tuple4<Long, String, Long, Long> in) throws Exception {
			return in.f3 != -1L;
		}
	}

	/**
	 * Flatmap function which applied the coordinates of the capital city of a country to the given language code. If no matching coordinates can be found, it will be initalized with latitude 0.0 and longitude 0.0 and also prints a warning
	 */
	public static class RetweetGeoFlatMap implements FlatMapFunction<Tuple4<Long, String, Long, Long>, Tuple5<String, Double, Double, Long, Integer>> {
		@Override
		public void flatMap(Tuple4<Long, String, Long, Long> retweet, Collector<Tuple5<String, Double, Double, Long, Integer>> collector) throws Exception {
			try {
				GeoCoordinates geo = Utils.langCodeToCoords(retweet.f1);
				collector.collect(new Tuple5<>(retweet.f1, geo.getLatitude(), geo.getLongitude(), retweet.f2, 1));
			} catch(GeoCoordinatesNotFoundException e) {
				logger.warn("Language code: " + retweet.f1 + " not found");
				collector.collect(new Tuple5<>(retweet.f1, 0.0, 0.0, retweet.f2, 1));
			}
		}
	}

	/**
	 * Map function for counting retweets
	 */
	public static class RetweetCountMap implements MapFunction<Tuple4<Long, String, Long, Long>, Tuple3<String, Long, Integer>> {
		@Override
		public Tuple3<String, Long, Integer> map(Tuple4<Long, String, Long, Long> in) throws Exception {
			return new Tuple3<>(in.f1, in.f2, 1);
		}
	}

	/**
	 * Group reduce for the generation of share of retweets per country according to the total amount of retweets
	 */
	public static class RetweetPercentageGroupReduce implements GroupReduceFunction<Tuple3<String, Long, Integer>, Tuple4<String, Long, Double, Integer>> {
		@Override
		public void reduce(Iterable<Tuple3<String, Long, Integer>> iterable, Collector<Tuple4<String, Long, Double, Integer>> collector) throws Exception {
			Integer total = 0;
			List<Tuple3<String, Long, Integer>> retweetList = new ArrayList<>();
			for (Tuple3<String, Long, Integer> countryRetweets : iterable) {
				total += countryRetweets.f2;
				retweetList.add(countryRetweets);
			}
			for (Tuple3<String, Long, Integer> countryRetweets : retweetList) {
				Double percentage = (double) countryRetweets.f2 / total * 100;
				percentage = Utils.roundDouble(percentage, decimalPlace);
				collector.collect(new Tuple4<>(countryRetweets.f0, countryRetweets.f1, percentage, countryRetweets.f2));
			}
		}
	}
}

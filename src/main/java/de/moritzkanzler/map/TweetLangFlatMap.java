package de.moritzkanzler.map;

import de.moritzkanzler.model.TwitterPostData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * This flatmap function checks if tweet has a valid user entry and gets the language of the tweet author for further processing
 */
public class TweetLangFlatMap implements FlatMapFunction<TwitterPostData, Tuple2<String, Integer>> {
    @Override
    public void flatMap(TwitterPostData tweet, Collector<Tuple2<String, Integer>> collector) throws Exception {
        if(tweet.hasUser()) {
            collector.collect(new Tuple2<>(tweet.getUser().getLang(), 1));
        }
    }
}

package de.moritzkanzler.map;

import de.moritzkanzler.model.TwitterPostData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

/**
 * This Flatmap function converts a TwitterPostData object into a 3-tuple to save it through as csv out of a stream
 */
public class TweetCsvMap implements MapFunction<TwitterPostData, Tuple3<Long, Long, Long>> {
    @Override
    public Tuple3<Long, Long, Long> map(TwitterPostData tweet) throws Exception {
        Long id = tweet.getId();
        Long watermark = tweet.getWatermark();
        Long retweetId = tweet.hasRetweet() ? tweet.getRetweetTwitterPostData().getId() : -1;

        return new Tuple3<>(id, watermark, retweetId);
    }
}
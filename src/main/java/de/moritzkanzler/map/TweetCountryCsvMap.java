package de.moritzkanzler.map;

import de.moritzkanzler.model.TwitterPostData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

/**
 * This Flatmap function converts a TwitterPostData object into a 4-tuple to save it through as csv out of a stream
 */
public class TweetCountryCsvMap implements FlatMapFunction<TwitterPostData, Tuple4<Long, String, Long, Long>> {
    @Override
    public void flatMap(TwitterPostData tweet, Collector<Tuple4<Long, String, Long, Long>> collector) throws Exception {
        Long id = tweet.getId();
        String lang = tweet.hasUser() ? tweet.getUser().getLang() : "no-lang";
        Long watermark = tweet.getWatermark();
        Long retweetId = tweet.hasRetweet() ? tweet.getRetweetTwitterPostData().getId() : -1;

        collector.collect(new Tuple4<>(id, lang, watermark, retweetId));
    }
}
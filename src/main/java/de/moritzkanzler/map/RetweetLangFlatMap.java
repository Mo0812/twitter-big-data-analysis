package de.moritzkanzler.map;

import de.moritzkanzler.model.TwitterPostData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * This Flatmap function checks first if a Tweet belongs to cited retweet, if so this retweet gets processed and the language code gets looked up
 */
public class RetweetLangFlatMap implements FlatMapFunction<TwitterPostData, Tuple2<String, Integer>> {
    @Override
    public void flatMap(TwitterPostData twitterPostData, Collector<Tuple2<String, Integer>> collector) throws Exception {
        if(twitterPostData.hasRetweet()) {
            TwitterPostData retweet = twitterPostData.getRetweetTwitterPostData();
            if(retweet.hasUser()) {
                collector.collect(new Tuple2<>(retweet.getUser().getLang(), 1));
            }
        }
    }
}


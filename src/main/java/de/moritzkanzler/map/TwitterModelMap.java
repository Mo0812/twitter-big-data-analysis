package de.moritzkanzler.map;

import de.moritzkanzler.helper.TwitterDataComposer;
import de.moritzkanzler.model.TwitterPostData;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * General map function which maps raw tweet data through the TwitterDataComposer class into a TwitterPostData object
 */
public class TwitterModelMap implements MapFunction<String, TwitterPostData> {

    @Override
    public TwitterPostData map(String s) throws Exception {
        return TwitterDataComposer.composeTwitterPostData(s);
    }
}

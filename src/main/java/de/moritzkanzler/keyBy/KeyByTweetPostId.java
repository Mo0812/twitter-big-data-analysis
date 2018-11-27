package de.moritzkanzler.keyBy;

import de.moritzkanzler.model.TwitterPostData;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * This KeySelector makes sure that the next key related process step handles the incoming data by the tweets ID
 */
public class KeyByTweetPostId implements KeySelector<TwitterPostData, Long> {
    @Override
    public Long getKey(TwitterPostData twitterPostData) throws Exception {
        return twitterPostData.getId();
    }
}

package de.moritzkanzler.helper;

import de.moritzkanzler.model.TwitterMediaData;
import de.moritzkanzler.model.TwitterPostData;
import de.moritzkanzler.model.TwitterUserData;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * This static class helps to bring the raw JSON data from flinks twitter data source into a defined model which can be operate more easily throughout the stream processes
 */
public class TwitterDataComposer {

    /**
     * Is post marked as deleted?
     * @param value
     * @return
     * @throws Exception
     */
    public static boolean isDeletedPost(String value) throws Exception {
        ObjectMapper jsonParser = new ObjectMapper();
        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
        return jsonNode.has("delete");
    }

    /**
     * Map raw JSON input of a twitter post into the TwitterPostData model
     * @param value
     * @return
     * @throws Exception
     */
    public static TwitterPostData composeTwitterPostData(String value) throws Exception {
        ObjectMapper jsonParser = new ObjectMapper();
        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

        long postId = 0L;
        if(jsonNode.has("id")) {
            postId = jsonNode.get("id").asLong();
        }

        String createdAt = "";
        if(jsonNode.has("created_at")) {
            createdAt = jsonNode.get("created_at").asText();
        }

        String text = "";
        if(jsonNode.has("text")) {
            text = jsonNode.get("text").asText();
        }

        long replyToStatusId = 0L;
        if(jsonNode.has("in_reply_to_status_id")) {
            replyToStatusId = jsonNode.get("in_reply_to_status_id").asLong();
        }

        long replyToUserId = 0L;
        if(jsonNode.has("in_reply_to_user_id")) {
            replyToUserId = jsonNode.get("in_reply_to_user_id").asLong();
        }

        int quoteCount = 0;
        if(jsonNode.has("quote_count")) {
            quoteCount = jsonNode.get("quote_count").asInt();
        }

        int replyCount = 0;
        if(jsonNode.has("reply_count")) {
            replyCount = jsonNode.get("reply_count").asInt();
        }

        int retweetCount = 0;
        if(jsonNode.has("retweet_count")) {
            retweetCount = jsonNode.get("retweet_count").asInt();
        }

        int favoriteCount = 0;
        if(jsonNode.has("favorite_count")) {
            favoriteCount = jsonNode.get("favorite_count").asInt();
        }

        String lang = "";
        if(jsonNode.has("lang")) {
            lang = jsonNode.get("lang").asText();
        }

        List<String> hashtags = new ArrayList<>();
        if(jsonNode.has("entities") && jsonNode.get("entities").has("hashtags") && jsonNode.get("entities").get("hashtags").has(0)) {
            for(JsonNode hashtag: jsonNode.get("entities").get("hashtags")) {
                hashtags.add(hashtag.get("text").asText());
            }
        }

        List<String> urls = new ArrayList<>();
        if(jsonNode.has("entities") && jsonNode.get("entities").has("urls") && jsonNode.get("entities").get("urls").has(0)) {
            for(JsonNode url: jsonNode.get("entities").get("urls")) {
                if(url.has("url")) {
                    urls.add(url.get("url").asText());
                }
            }
        }

        List<String> userMentions = new ArrayList<>();
        if(jsonNode.has("entities") && jsonNode.get("entities").has("user_mentions") && jsonNode.get("entities").get("user_mentions").has(0)) {
            for(JsonNode userMention: jsonNode.get("entities").get("user_mentions")) {
                userMentions.add(userMention.get("screen_name").asText());
            }
        }

        List<TwitterMediaData> media = new ArrayList<>();
        if(jsonNode.has("entities") && jsonNode.get("entities").has("media") && jsonNode.get("entities").get("media").has(0)) {
            for(JsonNode mediaNode: jsonNode.get("entities").get("media")) {
                TwitterMediaData tmd = TwitterDataComposer.composeTwitterMediaData(mediaNode.toString());
                media.add(tmd);
            }
        }

        TwitterPostData retweetTwitterPostData = null;
        if(jsonNode.has("retweeted_status")) {
            String retweetStatus = jsonNode.get("retweeted_status").toString();
            retweetTwitterPostData = TwitterDataComposer.composeTwitterPostData(retweetStatus);
        }

        return new TwitterPostData(postId, createdAt, text, TwitterDataComposer.composeTwitterUserData(value), replyToStatusId, replyToUserId, quoteCount, replyCount, retweetCount, favoriteCount, lang, hashtags, urls, userMentions, null, media, retweetTwitterPostData);
    }

    @Deprecated
    public static TwitterPostData composeTwitterPostData(Tuple10<Long, String, String, Long, Long, Integer, Integer, Integer, Integer, String> t) throws Exception {
        return new TwitterPostData(t.f0, t.f1, t.f2, null, t.f3, t.f4, t.f5, t.f6, t.f7, t.f8, t.f9, null, null, null, null, null, null);
    }

    /**
     * Map raw JSON input of a twitter user information into the TwitterUserData model
     * @param value
     * @return
     * @throws Exception
     */
    public static TwitterUserData composeTwitterUserData(String value) throws Exception {
        ObjectMapper jsonParser = new ObjectMapper();
        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
        
        TwitterUserData twitterUserData = null;

        if(jsonNode.has("user")) {
            JsonNode userNode = jsonNode.get("user");

            long id = 0L;
            if(userNode.has("id")) {
                id = userNode.get("id").asLong();
            }

            String name = "";
            if(userNode.has("name")) {
                name = userNode.get("name").asText();
            }

            String screenName = "";
            if(userNode.has("screen_name")) {
                screenName = userNode.get("screen_name").asText();
            }

            boolean verified = false;
            if(userNode.has("verified")) {
                verified = userNode.get("verified").asBoolean();
            }

            int followerCount = 0;
            if(userNode.has("followers_count")) {
                followerCount = userNode.get("followers_count").asInt();
            }

            int friendCount = 0;
            if(userNode.has("friends_count")) {
                friendCount = userNode.get("friends_count").asInt();
            }

            int favouritesCount = 0;
            if(userNode.has("favourites_count")) {
                favouritesCount = userNode.get("favourites_count").asInt();
            }

            int statusCount = 0;
            if(userNode.has("statuses_count")) {
                statusCount = userNode.get("statuses_count").asInt();
            }

            String memberSince = "";
            if(userNode.has("created_at")) {
                memberSince = userNode.get("created_at").asText();
            }

            String lang = "";
            if(userNode.has("lang")) {
                lang = userNode.get("lang").asText();
            }

            twitterUserData = new TwitterUserData(id, name, screenName, verified, followerCount, friendCount, favouritesCount, statusCount, memberSince, lang);
        }

        return twitterUserData;
    }

    /**
     * Map raw JSON input of a twitter media information into the TwitterMediaData model
     * @param value
     * @return
     * @throws Exception
     */
    private static TwitterMediaData composeTwitterMediaData(String value) throws Exception {
        ObjectMapper jsonParser = new ObjectMapper();
        JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

        TwitterMediaData twitterMediaData = null;

        long id = 0;
        if(jsonNode.has("id")) {
            id = jsonNode.get("id").asLong();
        }

        String mediaUrl = "";
        if(jsonNode.has("media_url")) {
            mediaUrl = jsonNode.get("media_url").asText();
        }

        String type = "";
        if(jsonNode.has("type")) {
            type = jsonNode.get("type").asText();
        }

        twitterMediaData = new TwitterMediaData(id, mediaUrl, type);

        return twitterMediaData;
    }
}

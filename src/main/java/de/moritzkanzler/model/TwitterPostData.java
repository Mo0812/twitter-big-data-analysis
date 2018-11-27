package de.moritzkanzler.model;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

/**
 * Model for tweets
 */
public class TwitterPostData {

    public long id;
    private String createdAt;
    private String text;
    private TwitterUserData user;
    private long replyToPostId;
    private long replayToUserId;
    private int qouteCount;
    private int replyCount;
    private int retweetCount;
    private int favoirteCount;
    private String lang;
    private List<String> hashtags;
    private List<String> urls;
    private List<String> userMentions;
    private String[] symbols;
    private List<TwitterMediaData> media;
    private TwitterPostData retweetTwitterPostData;

    public TwitterPostData(long id, String createdAt, String text, TwitterUserData user, long replyToPostId, long replayToUserId, int qouteCount, int replyCount, int retweetCount, int favoirteCount, String lang, List<String> hashtags, List<String> urls, List<String> userMentoions, String[] symbols, List<TwitterMediaData> media, TwitterPostData retweetTwitterPostData) {
        this.id = id;
        this.createdAt = createdAt;
        this.text = text;
        this.user = user;
        this.replyToPostId = replyToPostId;
        this.replayToUserId = replayToUserId;
        this.qouteCount = qouteCount;
        this.replyCount = replyCount;
        this.retweetCount = retweetCount;
        this.favoirteCount = favoirteCount;
        this.lang = lang;
        this.hashtags = hashtags;
        this.urls = urls;
        this.userMentions = userMentoions;
        this.symbols = symbols;
        this.media = media;
        this.retweetTwitterPostData = retweetTwitterPostData;
    }

    public long getId() {
        return id;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public String getText() {
        return text;
    }

    public TwitterUserData getUser() {
        return user;
    }

    public boolean hasUser() {
        return user != null;
    }

    public long getReplyToPostId() {
        return replyToPostId;
    }

    public long getReplayToUserId() {
        return replayToUserId;
    }

    public int getQouteCount() {
        return qouteCount;
    }

    public int getReplyCount() {
        return replyCount;
    }

    public int getRetweetCount() {
        return retweetCount;
    }

    public int getFavoirteCount() {
        return favoirteCount;
    }

    public String getLang() {
        return lang;
    }

    public List<String> getHashtags() {
        return hashtags;
    }

    public List<String> getUrls() {
        return urls;
    }

    public List<String> getUserMentions() {
        return userMentions;
    }

    public String[] getSymbols() {
        return symbols;
    }

    public List<TwitterMediaData> getMedia() {
        return media;
    }

    public boolean hasRetweet() {
        return retweetTwitterPostData != null;
    }

    public TwitterPostData getRetweetTwitterPostData() {
        return retweetTwitterPostData;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setUser(String TwitterUserData) {
        this.user = user;
    }

    public void setReplyToPostId(long replyToPostId) {
        this.replyToPostId = replyToPostId;
    }

    public void setReplayToUserId(long replayToUserId) {
        this.replayToUserId = replayToUserId;
    }

    public void setQouteCount(int qouteCount) {
        this.qouteCount = qouteCount;
    }

    public void setReplyCount(int replyCount) {
        this.replyCount = replyCount;
    }

    public void setRetweetCount(int retweetCount) {
        this.retweetCount = retweetCount;
    }

    public void setFavoirteCount(int favoirteCount) {
        this.favoirteCount = favoirteCount;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public void setHashtags(List<String> hashtags) {
        this.hashtags = hashtags;
    }

    public void setUrls(List<String> urls) {
        this.urls = urls;
    }

    public void setUserMentions(List<String> userMentions) {
        this.userMentions = userMentions;
    }

    public void setSymbols(String[] symbols) {
        this.symbols = symbols;
    }

    public void setMedia(List<TwitterMediaData> media) {
        this.media = media;
    }

    public void setRetweetTwitterPostData(TwitterPostData retweetTwitterPostData) {
        this.retweetTwitterPostData = retweetTwitterPostData;
    }

    public long getWatermark() {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("E MMM d H:m:s Z yyyy", Locale.US);
            Date parsedDate = simpleDateFormat.parse(this.createdAt);
            return parsedDate.getTime();
        } catch(Exception e) {
            System.err.println(e.getMessage());
            System.err.println("Error in date parsing.");
        }
        return 0;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TwitterPostData &&
                this.id == ((TwitterPostData) other).id;
    }

    @Override
    public int hashCode() {
        return (int)this.id;
    }
}

package de.moritzkanzler.model;

import scala.Int;

/**
 * Model for user informations in tweets
 */
public class TwitterUserData {

    private long id;
    private String name;
    private String screen_name;
    private String location;
    private String url;
    private String description;
    private boolean verified;
    private int followerCount;
    private int friendCount;
    private int listedCount;
    private int favouritesCount;
    private int statusCount;
    private String memberSince;
    private String lang;

    public TwitterUserData(long id, String name, String screen_name) {
        this.id = id;
        this.name = name;
        this.screen_name = screen_name;
    }


    public TwitterUserData(long id, String name, String screen_name, boolean verified, int followerCount, int friendCount, int favouritesCount, int statusCount, String memberSince, String lang) {
        this.id = id;
        this.name = name;
        this.screen_name = screen_name;
        this.verified = verified;
        this.followerCount = followerCount;
        this.friendCount = friendCount;
        this.favouritesCount = favouritesCount;
        this.statusCount = statusCount;
        this.memberSince = memberSince;
        this.lang = lang;
    }

    public TwitterUserData(long id, String name, String screen_name, String location, String url, String description, boolean verified, int followerCount, int friendCount, int listedCount, int favouritesCount, int statusCount, String memberSince, String lang) {
        this.id = id;
        this.name = name;
        this.screen_name = screen_name;
        this.location = location;
        this.url = url;
        this.description = description;
        this.verified = verified;
        this.followerCount = followerCount;
        this.friendCount = friendCount;
        this.listedCount = listedCount;
        this.favouritesCount = favouritesCount;
        this.statusCount = statusCount;
        this.memberSince = memberSince;
        this.lang = lang;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getScreen_name() {
        return screen_name;
    }

    public void setScreen_name(String screen_name) {
        this.screen_name = screen_name;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isVerified() {
        return verified;
    }

    public void setVerified(boolean verified) {
        this.verified = verified;
    }

    public int getFollowerCount() {
        return followerCount;
    }

    public void setFollowerCount(int followerCount) {
        this.followerCount = followerCount;
    }

    public int getFriendCount() {
        return friendCount;
    }

    public void setFriendCount(int friendCount) {
        this.friendCount = friendCount;
    }

    public int getListedCount() {
        return listedCount;
    }

    public void setListedCount(int listedCount) {
        this.listedCount = listedCount;
    }

    public int getFavouritesCount() {
        return favouritesCount;
    }

    public void setFavouritesCount(int favouritesCount) {
        this.favouritesCount = favouritesCount;
    }

    public int getStatusCount() {
        return statusCount;
    }

    public void setStatusCount(int statusCount) {
        this.statusCount = statusCount;
    }

    public String getMemberSince() {
        return memberSince;
    }

    public void setMemberSince(String memberSince) {
        this.memberSince = memberSince;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    @Override
    public String toString() {
        return Float.toString(id) + ";" + name + ";" + screen_name + ";" + location + ";" + url + ";" + description + ";" + (verified ? "1" : "0") + ";" + Integer.toString(followerCount) + ";" + Integer.toString(friendCount) + ";" + Integer.toString(listedCount) + ";" + Integer.toString(favouritesCount) + ";" + Integer.toString(statusCount) + ";" + memberSince + ";" + lang;
    }
}

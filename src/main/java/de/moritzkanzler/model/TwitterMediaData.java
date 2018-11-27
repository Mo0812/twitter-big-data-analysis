package de.moritzkanzler.model;

/**
 * Model for media informations in tweets
 */
public class TwitterMediaData {

    private long id;
    private String mediaUrl;
    private String type;

    public TwitterMediaData(long id, String mediaUrl, String type) {
        this.id = id;
        this.mediaUrl = mediaUrl;
        this.type = type;
    }

    public long getId() {
        return id;
    }

    public String getMediaUrl() {
        return mediaUrl;
    }

    public String getType() {
        return type;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setMediaUrl(String mediaUrl) {
        this.mediaUrl = mediaUrl;
    }

    public void setType(String type) {
        this.type = type;
    }
}

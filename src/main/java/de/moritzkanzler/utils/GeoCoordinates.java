package de.moritzkanzler.utils;

/**
 * Model for saving geo coordinates
 */
public class GeoCoordinates {

    private double latitude;
    private double longitude;

    public GeoCoordinates(Double latitude, Double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public GeoCoordinates(String latitude, String longitude) {
        this.latitude = Double.parseDouble(latitude.trim());
        this.longitude = Double.parseDouble(longitude.trim());
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }
}

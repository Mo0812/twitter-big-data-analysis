package de.moritzkanzler.exception;

/**
 * Custom exception to alert if a language code can not be mapped to the its country coordinates, when using the langcode to coordinate list.
 */
public class GeoCoordinatesNotFoundException extends Exception {
    public GeoCoordinatesNotFoundException() {
        super();
    }

    public GeoCoordinatesNotFoundException(String message) {
        super(message);
    }
}

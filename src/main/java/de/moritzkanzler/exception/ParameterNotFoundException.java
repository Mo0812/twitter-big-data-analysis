package de.moritzkanzler.exception;

/**
 * Custom exception to alert if a requested parameter in a properties file can not be found
 */
public class ParameterNotFoundException extends Exception {
    public ParameterNotFoundException() {
        super();
    }

    public ParameterNotFoundException(String message) {
        super(message);
    }
}

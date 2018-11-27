package de.moritzkanzler.helper;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.security.InvalidParameterException;

/**
 * Wrapper class for Twitter data source in apache flink jobs. It helps to write repetitive code to establish a twitter data input in the streaming jobs
 */
public class TwitterConnection {

    public static TwitterSource getTwitterConnection() throws InvalidParameterException, Exception {
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("src/main/resources/twitter.properties");

        if(!(parameterTool.has(TwitterSource.CONSUMER_KEY) &&
                parameterTool.has(TwitterSource.CONSUMER_SECRET) &&
                parameterTool.has(TwitterSource.TOKEN) &&
                parameterTool.has(TwitterSource.TOKEN_SECRET)
        )) {
            throw new InvalidParameterException("Twitter credentials not found or defined correctly.");
        }

        return new TwitterSource(parameterTool.getProperties());
    }
}

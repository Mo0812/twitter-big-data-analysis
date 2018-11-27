package de.moritzkanzler.filter;

import de.moritzkanzler.helper.TwitterDataComposer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * General filter function to ignore post in the twitter sink which are marked as deleted
 */
public class IgnoreDeletedPostsFilter implements FilterFunction<String> {
    @Override
    public boolean filter(String value) throws Exception {
        return !TwitterDataComposer.isDeletedPost(value);
    }
}

package de.moritzkanzler.keyBy;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * This KeySelector makes sure that the next key related process step handles all incoming data as they belong to the same key
 * @param <T>
 */
public class KeyByAllDistinct<T> implements KeySelector<T, Long> {
    @Override
    public Long getKey(T t) throws Exception {
        return 0L;
    }
}

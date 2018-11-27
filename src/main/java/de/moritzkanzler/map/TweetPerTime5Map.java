package de.moritzkanzler.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.Calendar;
import java.util.Date;

/**
 * Map function to map a given publish date of a tweet into a time frame in minutes set through windowSequence
 */
public class TweetPerTime5Map implements MapFunction<Tuple5<Long, Long, Long, Long, String>, Tuple5<Long, Long, Long, Long, String>> {

    private int windowSequence;

    public TweetPerTime5Map(int windowSequence) {
        this.windowSequence = windowSequence;
    }

    @Override
    public Tuple5<Long, Long, Long, Long, String> map(Tuple5<Long, Long, Long, Long, String> t) throws Exception {
        Date date = new Date(t.f1);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.SECOND, 0);

        int minute = calendar.get(Calendar.MINUTE);

        Long newTimestamp = calendar.getTime().getTime();
        return new Tuple5<>(t.f0, newTimestamp, t.f2, t.f3, t.f4);
    }
}
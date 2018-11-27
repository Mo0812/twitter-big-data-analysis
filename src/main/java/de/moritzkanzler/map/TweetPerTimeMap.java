package de.moritzkanzler.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.Calendar;
import java.util.Date;

/**
 * Map function to map a given publish date of a tweet into a time frame in minutes set through windowSequence
 */
public class TweetPerTimeMap implements MapFunction<Tuple4<Long, String, Long, Long>, Tuple4<Long, String, Long, Long>> {

    private int windowSequence;

    public TweetPerTimeMap(int windowSequence) {
        this.windowSequence = windowSequence;
    }

    @Override
    public Tuple4<Long, String, Long, Long> map(Tuple4<Long, String, Long, Long> t) throws Exception {
        Date date = new Date(t.f2);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.SECOND, 0);

        int minute = calendar.get(Calendar.MINUTE);
        int diff = (int) minute / windowSequence;
        int newMinute = diff * 15;

        calendar.set(Calendar.MINUTE, newMinute);
        Long newTimestamp = calendar.getTime().getTime();
        return new Tuple4<>(t.f0, t.f1, newTimestamp, t.f3);
    }
}
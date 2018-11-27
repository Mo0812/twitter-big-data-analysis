package de.moritzkanzler.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Calendar;
import java.util.Date;

/**
 * Map function to map a given publish date of a tweet into a time frame in minutes set through windowSequence
 */
public class TweetPerTime3Map implements MapFunction<Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>> {

    private int windowSequence;

    public TweetPerTime3Map(int windowSequence) {
        this.windowSequence = windowSequence;
    }

    @Override
    public Tuple3<Long, Long, Long> map(Tuple3<Long, Long, Long> t) throws Exception {
        Date date = new Date(t.f1);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.SECOND, 0);

        int minute = calendar.get(Calendar.MINUTE);
        int diff = (int) minute / windowSequence;
        int newMinute = diff * windowSequence;

        calendar.set(Calendar.MINUTE, newMinute);
        Long newTimestamp = calendar.getTime().getTime();
        return new Tuple3<>(t.f0, newTimestamp, t.f2);
    }
}
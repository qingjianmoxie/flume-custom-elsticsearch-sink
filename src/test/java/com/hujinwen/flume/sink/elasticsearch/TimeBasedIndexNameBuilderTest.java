package com.hujinwen.flume.sink.elasticsearch;

import org.apache.commons.lang.time.FastDateFormat;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class TimeBasedIndexNameBuilderTest {

    private static final FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd", TimeZone.getTimeZone("Etc/UTC"));

    @Test
    public void test1() {
        final Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.DAY_OF_MONTH, 7);

        String result = "flume" + "-" + fastDateFormat.format(calendar);

        System.out.println();
    }

}
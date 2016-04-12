package com.mgm.ring.functions;

import org.apache.flink.api.common.functions.MapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * class extracting the timestamp of a given tweet
 *
 * @author Martin Grimmer (martin.grimmer@mgm-tp.com)
 */
public class DateExtraction implements MapFunction<String, Tuple2<Long, String>>, Serializable {

    // example time string: "Wed Mar 23 12:01:40 +0000 2016"
    public static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy");

    /**
     * empty constructor for serialization (needed by flink)
     */
    public DateExtraction() {
    }

    @Override
    /**
     * extracts the timestamp and converts it into a long (seconds since 1970)
     */
    public Tuple2<Long, String> map(String tweetJson) throws Exception {
        int pos1 = tweetJson.indexOf("\"created_at\":\"");
        int pos2 = pos1 + 14;
        int pos3 = tweetJson.indexOf("\",\"", pos2);
        String rawTime = tweetJson.substring(pos2, pos3);
        ZonedDateTime time = ZonedDateTime.parse(rawTime, formatter);
        long key = time.toEpochSecond();
        return new Tuple2(key, tweetJson);
    }
}

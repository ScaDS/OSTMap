package org.iidp.ostmap.stream_processing.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * class extracting the timestamp of a given tweet
 */
public class DateExtraction implements FlatMapFunction<String, Tuple2<Long, String>>, Serializable {

    // example time string: "Wed Mar 23 12:01:40 +0000 2016"  'Thu Apr 28 09:17:52 +0000 2016'
    public static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy").withLocale(Locale.ENGLISH);

    /**
     * empty constructor for serialization (needed by flink)
     */
    public DateExtraction() {
    }

    /**
     *
     * @param tweetJson tweet as json-string
     * @param out Tuple of timestamp and tweet
     * @throws Exception
     */
    @Override
    public void flatMap(String tweetJson, Collector<Tuple2<Long, String>> out) throws Exception {

        int pos1 = tweetJson.indexOf("\"created_at\":\"");
        int pos2 = pos1 + 14;
        int pos3 = tweetJson.indexOf("\",\"", pos2);
        if (pos1 != -1 && pos2 != -1) {
            String rawTime = tweetJson.substring(pos2, pos3);
            ZonedDateTime time = ZonedDateTime.parse(rawTime, formatter);
            long key = time.toEpochSecond();
            out.collect(new Tuple2(key, tweetJson));
        }
    }
}

package org.iidp.ostmap.stream_processing.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * class extracting the timestamp of a given tweet
 *
 * @author Martin Grimmer (martin.grimmer@mgm-tp.com)
 */
public class UserExtraction implements FlatMapFunction<Tuple2<Long, String>, Tuple3<Long, String, String>>, Serializable {

    // example time string: "Wed Mar 23 12:01:40 +0000 2016"  'Thu Apr 28 09:17:52 +0000 2016'
    public static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy").withLocale(Locale.ENGLISH);

    /**
     * empty constructor for serialization (needed by flink)
     */
    public UserExtraction() {
    }

    @Override
    public void flatMap(Tuple2<Long, String> input, Collector<Tuple3<Long, String, String>> out) throws Exception {

        int posUserArea = input._2().indexOf("\"user\":{");

        int pos1 = input._2().indexOf("\"name\":\"", posUserArea);
        int pos2 = pos1 + 8;
        int pos3 = input._2().indexOf("\",\"", pos2);
        if (posUserArea != -1 && pos1 != -1 && pos2 != -1) {
            String user = input._2().substring(pos2, pos3);
            out.collect(new Tuple3(input._1(), input._2(), user));
        }
    }
}

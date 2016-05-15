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
 * class extracting the languageTag of a given tweet
 */
public class LanguageTagExtraction implements FlatMapFunction<Tuple3<Long, String, String>, Tuple3<Long, String, String>>, Serializable {

    /**
     * empty constructor for serialization (needed by flink)
     */
    public LanguageTagExtraction() {
    }

    /**
     *
     * @param input Tuple of timestamp, tweet as json-string and timeString
     * @param out Tuple of timestamp, languageTag and timeString
     * @throws Exception
     */
    @Override
    public void flatMap(Tuple3<Long, String, String> input, Collector<Tuple3<Long, String, String>> out) throws Exception {
        String tweetJson = input._2();
        String langTag = extractLangTag(tweetJson);
        if(langTag!=null) {
            out.collect(new Tuple3(input._1(), langTag, input._3()));
        }
    }


    public String extractLangTag(String json) {
        int pos1 = json.indexOf("\"lang\":\"");
        int pos2 = pos1 + 8;
        int pos3 = json.indexOf("\",\"", pos2);
        if (pos1 > -1 && pos2 > -1 && pos3 > -1) {
            return json.substring(pos2, pos3).toLowerCase();
        } else return null;
    }


}

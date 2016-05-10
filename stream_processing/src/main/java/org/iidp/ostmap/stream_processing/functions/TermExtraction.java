package org.iidp.ostmap.stream_processing.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.iidp.ostmap.stream_processing.types.RawTwitterDataKey;
import org.iidp.ostmap.stream_processing.types.TermIndexKey;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;

import org.iidp.ostmap.commons.tokenizer.*;

/**
 * class extracting the user of a given tweet
 */
public class TermExtraction implements FlatMapFunction<Tuple2<RawTwitterDataKey, String>, Tuple2<TermIndexKey, Integer>>, Serializable {

    /**
     * empty constructor for serialization (needed by flink)
     */
    public TermExtraction() {
    }

    /**
     * Extract user for each tweet
     * @param input     tuple of rawTwitterDataKey and tweet-json
     * @param out       tuple of termIndexKey and occurence count
     * @throws Exception
     */
    @Override
    public void flatMap(Tuple2<RawTwitterDataKey, String> input, Collector<Tuple2<TermIndexKey, Integer>> out) throws Exception {

        // build basic key with empty token
        TermIndexKey tiKey = TermIndexKey.buildTermIndexKey("", TermIndexKey.SOURCE_TYPE_TEXT, input._1);

        //Collect all text-entries
        String text = getText(input._2());
        Tokenizer tokenizer = new Tokenizer();
        ArrayList<String> list = (ArrayList<String>) tokenizer.tokenizeString(text);
        for(String token : list)
        {
            tiKey.setTerm(token);
            int count = Collections.frequency(list, token);
            out.collect(new Tuple2<>(tiKey, count));
        }

    }

    /**
     * Creating substring containing only the tweet's text
     *
     * @param tweetJson     whole tweet-json
     * @return tweet's text
     */
    public String getText(String tweetJson)
    {
        int pos1 = tweetJson.indexOf("\"text\":\"");
        int pos2 = pos1 + 8;
        int pos3 = tweetJson.indexOf("\",\"", pos2);
        return tweetJson.substring(pos2, pos3);
    }
}

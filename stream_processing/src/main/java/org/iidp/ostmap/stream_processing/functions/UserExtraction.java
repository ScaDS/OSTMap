package org.iidp.ostmap.stream_processing.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.iidp.ostmap.stream_processing.types.RawTwitterDataKey;
import org.iidp.ostmap.stream_processing.types.TermIndexKey;
import scala.Tuple2;

import java.io.Serializable;

/**
 * class extracting the user-data of a given tweet
 */
public class UserExtraction implements FlatMapFunction<Tuple2<RawTwitterDataKey, String>, Tuple2<TermIndexKey, Integer>>, Serializable {

    /**
     * empty constructor for serialization (needed by flink)
     */
    public UserExtraction() {
    }

    /**
     * Extract user for each tweet
     * @param input     tuple of rawTwitterDataKey and tweet-json
     * @param out       tuple of termIndexKey and occurence count
     * @throws Exception
     */
    @Override
    public void flatMap(Tuple2<RawTwitterDataKey, String> input, Collector<Tuple2<TermIndexKey, Integer>> out) throws Exception {

        int posUserArea = input._2().indexOf("\"user\":{");
        TermIndexKey tiKey;
        String term;

        // extract user's name
        int pos1 = input._2().indexOf("\"name\":\"", posUserArea);
        int pos2 = pos1 + 8;
        int pos3 = input._2().indexOf("\",\"", pos2);
        if (posUserArea > -1 && pos1 > -1 && pos2 > -1 && pos3 > -1) {
            term = input._2().substring(pos2, pos3).toLowerCase();
            tiKey = TermIndexKey.buildTermIndexKey(term, TermIndexKey.SOURCE_TYPE_USER, input._1);
            out.collect(new Tuple2(tiKey, 0));
        }

        // extract user's screen_name
        pos1 = input._2().indexOf("\"screen_name\":\"", posUserArea);
        pos2 = pos1 + 15;
        pos3 = input._2().indexOf("\",\"", pos2);
        if (posUserArea > -1 && pos1 > -1 && pos2 > -1 && pos3 > -1) {
            term = input._2().substring(pos2, pos3).toLowerCase();
            tiKey = TermIndexKey.buildTermIndexKey(term, TermIndexKey.SOURCE_TYPE_USER, input._1);
            out.collect(new Tuple2(tiKey, 0));
        }
    }
}

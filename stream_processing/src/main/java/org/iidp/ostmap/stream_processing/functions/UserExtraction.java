package org.iidp.ostmap.stream_processing.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.iidp.ostmap.commons.accumulo.keys.RawTwitterDataKey;
import org.iidp.ostmap.commons.accumulo.keys.TermIndexKey;
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

        TermIndexKey tiKey;
        JSONObject obj;
        String userName;
        String userScreenName;

        try {
            obj = new JSONObject(input._2());
            userScreenName = obj.getJSONObject("user").getString("screen_name").toLowerCase();
            userName = obj.getJSONObject("user").getString("name").toLowerCase();

            tiKey = TermIndexKey.buildTermIndexKey(userName, TermIndexKey.SOURCE_TYPE_USER, input._1);
            out.collect(new Tuple2(tiKey, 0));
            tiKey = TermIndexKey.buildTermIndexKey(userScreenName, TermIndexKey.SOURCE_TYPE_USER, input._1);
            out.collect(new Tuple2(tiKey, 0));

        } catch (JSONException e) {
            e.printStackTrace();
        }
    }
}

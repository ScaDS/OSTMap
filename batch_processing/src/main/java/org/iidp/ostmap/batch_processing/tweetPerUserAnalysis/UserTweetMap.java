package org.iidp.ostmap.batch_processing.tweetPerUserAnalysis;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * maps RawTwitterData to tuples of userNameString and tweetCount
 */
public class UserTweetMap implements MapFunction<Tuple2<Key, Value>, Tuple2<String,Integer>> {
    @Override
    public Tuple2<String, Integer> map(Tuple2<Key, Value> in) throws Exception {

        JSONObject obj = null;
        String userName = "";

        try {
            obj = new JSONObject(in.f1.toString());
            userName = obj.getJSONObject("user").getString("name");

        } catch (JSONException e) {
            e.printStackTrace();
        }

        return new Tuple2<>(userName,1);
    }
}

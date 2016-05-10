package org.iidp.ostmap.commons.areacalc;


import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import java.io.Serializable;

public class GeoExtrationFlatMap implements FlatMapFunction<Tuple2<Key, Value>, Tuple2<String,String>>, Serializable {

    public GeoExtrationFlatMap(){
    }

    @Override
    public void flatMap(Tuple2<Key, Value> in, Collector<Tuple2<String, String>> out) {

        JSONObject obj = null;
        String userName = "";
        String text = "";
        int geoX = 0;
        int geoY = 0;

        try {
            obj = new JSONObject(in.f1.toString());
            text = obj.getString("text");
            userName = obj.getJSONObject("user").getString("name");

        } catch (JSONException e) {
            e.printStackTrace();
        }

        //create mutations for tokens
        out.collect(new Tuple2<>(userName,geoX + "|" + geoY));


    }
}

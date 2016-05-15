package org.iidp.ostmap.batch_processing.areacalc;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.Serializable;


public class GeoCalcFlatMap implements FlatMapFunction<Tuple2<String,String>, Tuple2<String,String>> {

    public GeoCalcFlatMap(){
    }

    @Override
    public void flatMap(Tuple2<String, String> in, Collector<Tuple2<String, String>> out) {

        JSONObject obj = null;
        String userName = "";
        String coordinates = "";

        try {
            obj = new JSONObject(in.f1.toString());
            userName = obj.getJSONObject("user").getString("name");
            coordinates = obj.getString("coordinates");
            //coordinates = obj.getJSONObject("coordinates").getString("coordinates");
            /**
             * TODO: fallback location und geo auswerten falls es keine coordinates gibt
             */

        } catch (JSONException e) {
            e.printStackTrace();
        }


        out.collect(new Tuple2<>(userName,coordinates));


    }
}

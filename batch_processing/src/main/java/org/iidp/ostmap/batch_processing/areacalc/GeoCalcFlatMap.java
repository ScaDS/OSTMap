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
        String userName = in.f0;
        String coordinates = "";

        try {

        } catch (Exception e) {
            e.printStackTrace();
        }


        out.collect(new Tuple2<>(userName,coordinates));


    }
}

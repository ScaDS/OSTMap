package org.iidp.ostmap.batch_processing.areacalc;


import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.codehaus.jettison.json.JSONObject;
import org.iidp.ostmap.commons.extractor.Extractor;

import java.io.Serializable;

public class GeoExtrationFlatMap implements FlatMapFunction<Tuple2<Key, Value>, Tuple2<String, String>>, Serializable {


    public GeoExtrationFlatMap() {
    }

    /**
     * Extracts the Coordinates from any given tweet
     *
     * @param in  Entries from the Rawtwittertable
     * @param out extracted data
     */
    @Override
    public void flatMap(Tuple2<Key, Value> in, Collector<Tuple2<String, String>> out) {
        try {
            JSONObject obj = new JSONObject(in.f1.toString());
            String userName = "";
            Double[] coordinates = Extractor.extractLocation(obj);
            userName = obj.getJSONObject("user").getString("screen_name");
            out.collect(new Tuple2<>(userName, coordinates[0] + "," + coordinates[1]));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

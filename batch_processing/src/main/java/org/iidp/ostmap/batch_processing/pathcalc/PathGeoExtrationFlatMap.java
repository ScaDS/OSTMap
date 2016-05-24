package org.iidp.ostmap.batch_processing.pathcalc;


import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.iidp.ostmap.commons.extractor.Extractor;

import java.io.Serializable;

public class PathGeoExtrationFlatMap implements FlatMapFunction<Tuple2<Key, Value>, Tuple2<String, String>>, Serializable {


    public PathGeoExtrationFlatMap() {
    }

    /**
     * Extracts the Coordinates and the timestamp from any given tweet
     *
     * @param in  Entries from the Rawtwittertable
     * @param out extracted data
     */
    @Override
    public void flatMap(Tuple2<Key, Value> in, Collector<Tuple2<String, String>> out) {
        try {
            JSONObject obj = new JSONObject(in.f1.toString());
            ;
            JSONObject toReturn = null;
            String userName = "";
            Double[] coordinates = Extractor.extractLocation(obj);
            Long timestamp = 0L;

            userName = obj.getJSONObject("user").getString("screen_name");
            timestamp = obj.getLong("timestamp_ms");
            toReturn = new JSONObject();
            toReturn.append("timestamp_ms", timestamp);
            JSONArray coords = new JSONArray();
            coords.put(coordinates[0]);
            coords.put(coordinates[1]);
            toReturn.append("coordinates", coords);
            out.collect(new Tuple2<>(userName, toReturn.toString()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

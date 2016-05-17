package org.iidp.ostmap.batch_processing.areacalc;


import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import java.io.Serializable;

public class GeoExtrationFlatMap implements FlatMapFunction<Tuple2<Key, Value>, Tuple2<String,String>>, Serializable {

    public GeoExtrationFlatMap(){
    }

    /**
     * Extracts the Coordinates from any given tweet
     * @param in Entries from the Rawtwittertable
     * @param out extracted data
     */
    @Override
    public void flatMap(Tuple2<Key, Value> in, Collector<Tuple2<String, String>> out) {

        JSONObject obj = null;
        String userName = "";
        String coordinates = "";

        try {
            obj = new JSONObject(in.f1.toString());
            userName = obj.getJSONObject("user").getString("screen_name");
            if(!obj.get("geo").equals(null)){
                coordinates = obj.getJSONArray("geo").get(0).toString() + "," + obj.getJSONArray("geo").get(1).toString();
            }else if(!obj.get("coordinates").equals(null)){
                coordinates = obj.getJSONArray("coordinates").get(0).toString() + "," + obj.getJSONArray("coordinates").get(1).toString();
            }else if(!obj.get("place").equals(null)){
                JSONArray coords = obj.getJSONObject("place").getJSONObject("bounding_box").getJSONArray("coordinates").getJSONArray(0);
                /**
                 * JSONArray is not iterable
                 */
                double xCoord = 0.0;
                double yCoord = 0.0;
                for (int i = 0; i < 4; i++) {
                    xCoord += coords.getJSONArray(i).getDouble(0);
                    yCoord += coords.getJSONArray(i).getDouble(1);
                }
                coordinates = (xCoord/4) + "," + (yCoord/4);
            }else{
                // TODO errorhandling
            }


        } catch (JSONException e) {
            e.printStackTrace();
        }


        out.collect(new Tuple2<>(userName,coordinates));


    }
}

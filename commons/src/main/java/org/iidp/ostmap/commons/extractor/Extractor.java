package org.iidp.ostmap.commons.extractor;

import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Created by schotti on 11.05.16.
 */
public class Extractor {

    public static Double[] extractLocation(String json) {

        JSONObject obj = null;
        Double longitude = 0.0;
        Double latitude = 0.0;
        try {
            obj = new JSONObject(json);
            if(!obj.isNull("coordinates")) {
                if (!obj.getJSONObject("coordinates").isNull("coordinates")) {
                    JSONArray coords = obj.getJSONObject("coordinates").getJSONArray("coordinates");

                    longitude = coords.getDouble(0);
                    latitude = coords.getDouble(1);

                }
            } else {
                System.out.println("In places");
                JSONArray places = obj.getJSONObject("place").getJSONObject("bounding_box").getJSONArray("coordinates").getJSONArray(0);

                   if (places.length() > 2) {
                       Double topLeftLong = places.getJSONArray(0).getDouble(0);
                       Double topLeftLat = places.getJSONArray(0).getDouble(1);

                       Double lowerLeftLong = places.getJSONArray(1).getDouble(0);
                       Double lowerLeftLat = places.getJSONArray(1).getDouble(1);

                       Double topRightLong = places.getJSONArray(2).getDouble(0);
                       Double topRightLat = places.getJSONArray(2).getDouble(1);

                       Double lowerRightLong = places.getJSONArray(3).getDouble(0);
                       Double lowerRightLat = places.getJSONArray(3).getDouble(1);

                       longitude = (topLeftLong + lowerLeftLong + topRightLong + lowerRightLong) / 4;
                       latitude = (topLeftLat + lowerLeftLat + topRightLat + lowerRightLat) / 4;
                    }
            }
            Double[] longLat = {longitude, latitude};
            return longLat;


        } catch (JSONException e) {
            System.err.println("No Correct JSON File");
            e.printStackTrace();
            return null;
        }

    }
}


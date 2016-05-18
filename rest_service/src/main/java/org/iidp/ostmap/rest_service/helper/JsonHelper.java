package org.iidp.ostmap.rest_service.helper;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.iidp.ostmap.commons.extractor.Extractor;

public class JsonHelper {

    public static String generateCoordinates(String json) {
        JSONObject obj = null;
        Double[] longLat = Extractor.extractLocation(json);
        try {
            obj = new JSONObject(json);
            if(obj.has("coordinates") && obj.isNull("coordinates")) {
                //coordinates is empty
                JSONArray longLatArr = new JSONArray();
                if (longLat != null && longLat[0] != null && longLat[1] != null) {
                    longLatArr.put(longLat[0]);
                    longLatArr.put(longLat[1]);
                    JSONObject coordinates1 = new JSONObject().put("coordinates", longLatArr);
                    obj.put("coordinates",coordinates1);
                }
            }
            if(obj.has("place")){
                //ccordinates is set, remove place
                obj.remove("place");
            }
            return obj.toString();
        } catch (JSONException e) {
            System.err.println("No Correct JSON File");
            return null;
        }
    }
}

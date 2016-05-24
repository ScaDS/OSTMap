package org.iidp.ostmap.rest_service.helper;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.iidp.ostmap.commons.extractor.Extractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonHelper {

    static Logger log = LoggerFactory.getLogger(JsonHelper.class);

    public static String generateCoordinates(String json) {
        try {
            JSONObject obj = new JSONObject(json);
            Double[] longLat = Extractor.extractLocation(obj);
            if (obj.has("coordinates") && obj.isNull("coordinates")) {
                //coordinates is empty
                JSONArray longLatArr = new JSONArray();
                if (longLat != null && longLat[0] != null && longLat[1] != null) {
                    longLatArr.put(longLat[0]);
                    longLatArr.put(longLat[1]);
                    JSONObject coordinates1 = new JSONObject().put("coordinates", longLatArr);
                    obj.put("coordinates", coordinates1);
                }
            }
            if (obj.has("place")) {
                //coordinates is set, remove place
                obj.remove("place");
            }
            return obj.toString();
        } catch (JSONException e) {
            log.error("no correct JSON string:" + json);
            return null;
        }
    }
}

package org.iidp.ostmap.accumuloiterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.nio.charset.StandardCharsets;

/**
 * this iterator extracts the following fields and assembles them into a new json:
 * - text, user.name, user.screen_name,user.profile_image_url, coordinates, created_at, place.bounding_box.coordinates
 * The original json structure will be untouched.
 */
public class ExtractIterator extends WrappingIterator {

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        ExtractIterator copy = null;
        try {
            copy = this.getClass().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return copy;
    }

    @Override
    public Value getTopValue() {
        Value originalValue = super.getTopValue();

        Value reducedValue = null;
        try {
            reducedValue = new Value(reduceJson(originalValue.toString()).getBytes(StandardCharsets.UTF_8));
        } catch (JSONException je) {
            reducedValue = originalValue;
        }
        return reducedValue;
    }

    /**
     * returns a reduced string of the given json string
     *
     * @param originalString
     * @return
     */
    String reduceJson(String originalString) throws JSONException {

        JSONObject originalJson = null;
        JSONObject reducedJson = new JSONObject();

        originalJson = new JSONObject(originalString);

        // extract created_at

        reducedJson.put("created_at", originalJson.get("created_at"));

        // extract text
        reducedJson.put("text", originalJson.get("text"));

        // user
        JSONObject originalUser = originalJson.getJSONObject("user");
        JSONObject reducedUser = new JSONObject();
        reducedUser.put("name", originalUser.get("name"));
        reducedUser.put("screen_name", originalUser.get("screen_name"));
        reducedUser.put("profile_image_url", originalUser.get("profile_image_url"));
        reducedJson.put("user", reducedUser);

        // extract coordinates
        if (!originalJson.isNull("coordinates")
                && !originalJson.getJSONObject("coordinates").isNull("coordinates")) {
            JSONArray originalCoordinates = originalJson.getJSONObject("coordinates").getJSONArray("coordinates");
            JSONObject reducedCoordinates = new JSONObject().put("coordinates",originalCoordinates);
            reducedJson.put("coordinates", reducedCoordinates);
        } else {
            reducedJson.put("coordinates", JSONObject.NULL);
        }

        // extract place box coordinates
        if (!originalJson.isNull("place")
                && !originalJson.getJSONObject("place").isNull("bounding_box")
                && !originalJson.getJSONObject("place").getJSONObject("bounding_box").isNull("coordinates")) {
            JSONArray originalPlaceBb = originalJson.getJSONObject("place").getJSONObject("bounding_box").getJSONArray("coordinates");
            JSONObject reducedCoordinates = new JSONObject().put("coordinates", originalPlaceBb);
            JSONObject reducedBb = new JSONObject().put("bounding_box", reducedCoordinates);
            reducedJson.put("place", reducedBb);
        } else {
            reducedJson.put("place", JSONObject.NULL);
        }
        return reducedJson.toString();
    }
}

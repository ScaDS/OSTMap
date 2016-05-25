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
 * - coordinates, id_str
 * The original json structure will be untouched.
 */
public class GeosearchExtractIterator extends WrappingIterator {

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        GeosearchExtractIterator copy = null;
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


        // extract id_str
        reducedJson.put("id_str", originalJson.get("id_str"));

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

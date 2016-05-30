package org.iidp.ostmap.rest_service.helper;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.iidp.ostmap.commons.extractor.Extractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JsonHelper {

    public static final String KEY_COORDINATES = "coordinates";
    public static final String KEY_PLACE = "place";
    public static final String KEY_TWEETS = "tweets";
    public static final String KEY_TOPTEN = "topten";
    static Logger log = LoggerFactory.getLogger(JsonHelper.class);

    public static String generateCoordinates(String json) {
        try {
            JSONObject obj = new JSONObject(json);
            Double[] longLat = Extractor.extractLocation(obj);

            if (obj.has(KEY_COORDINATES) && obj.isNull(KEY_COORDINATES)) {
                //coordinates is empty
                JSONArray longLatArr = new JSONArray();
                if (longLat != null && longLat[0] != null && longLat[1] != null) {
                    longLatArr.put(longLat[0]);
                    longLatArr.put(longLat[1]);
                    JSONObject coordinates1 = new JSONObject().put(KEY_COORDINATES, longLatArr);
                    obj.put(KEY_COORDINATES, coordinates1);
                }
            }
            if (obj.has(KEY_PLACE)) {
                //ccordinates is set, remove place
                obj.remove(KEY_PLACE);
            }
            return obj.toString();
        } catch (JSONException e) {
            log.error("no correct JSON string:" + json);

            return null;
        }
    }

    /**
     * Creates the Response for the webservice: A json object with two elements - "tweets" (json array with the tweets) and "topten" (json array with top ten hashtags as string)
     *
     * @param tweetsArray json array with the accumulo result tweets as string
     * @return the json object as string
     * @throws RuntimeException
     */
    public static String createTweetsWithHashtagRanking(String tweetsArray) throws RuntimeException {
        JSONObject returnJsonObj = new JSONObject();
        try {

            JSONArray tweetsJsonArray = new JSONArray(tweetsArray);

            returnJsonObj.put(KEY_TWEETS, tweetsJsonArray);
            returnJsonObj.put(KEY_TOPTEN, extractTopTenHashtags(tweetsArray));

        } catch (JSONException e) {
            throw new RuntimeException("Failure during create json object with hashtag search.");
        }
        return returnJsonObj.toString();
    }

    /**
     * Creates the Response for the webservice: A json object with one elements - "tweets" (json array with the tweets)
     *
     * @param tweetsArray json array with the accumulo result tweets as string
     * @return the json object as string
     * @throws RuntimeException
     */
    public static String createTweetsWithoutHashtagRanking(String tweetsArray) throws RuntimeException {
        JSONObject returnJsonObj = new JSONObject();
        try {

            JSONArray tweetsJsonArray = new JSONArray(tweetsArray);
            returnJsonObj.put(KEY_TWEETS, tweetsJsonArray);

        } catch (JSONException e) {
            throw new RuntimeException("Failure during create json object.");
        }
        return returnJsonObj.toString();
    }

    static JSONArray extractTopTenHashtags(String tweetsArray) {
        JSONArray toptenArray = new JSONArray();
        Map<String, Integer> hm = new TreeMap<>();

        Pattern p = Pattern.compile("#\\w+");
        Matcher m = p.matcher(tweetsArray);

        while (m.find()) {
            String tag = m.group();
            tag = tag.toLowerCase();
            if (!hm.containsKey(tag)) {
                hm.put(tag, 1);
            } else {
                hm.put(tag, (Integer) hm.get(tag) + 1);
            }
        }

        Map sortedMap = sortByValues(hm);

        // Get a set of the entries on the sorted map
        Set set = sortedMap.entrySet();

        // Get an iterator
        Iterator iterator = set.iterator();

        int counter = 1;
        // Display elements
        while (iterator.hasNext() && counter <= 10) {
            counter++;
            Map.Entry me = (Map.Entry) iterator.next();
            toptenArray.put(me.getKey());
        }
        return toptenArray;
    }

    private static <K, V extends Comparable<V>> Map<K, V> sortByValues(final Map<K, V> map) {
        Comparator<K> valueComparator = new Comparator<K>() {
            public int compare(K k1, K k2) {
                int compare = map.get(k2).compareTo(map.get(k1));
                if (compare == 0) return 1;
                else return compare;
            }
        };
        Map<K, V> sortedByValues = new TreeMap<K, V>(valueComparator);
        sortedByValues.putAll(map);
        return sortedByValues;
    }
}
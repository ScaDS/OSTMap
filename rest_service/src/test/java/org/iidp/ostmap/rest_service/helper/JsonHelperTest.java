package org.iidp.ostmap.rest_service.helper;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Objects;

import static org.junit.Assert.*;

public class JsonHelperTest {

    private static String tweetWithoutCoord;
    private static String tweetsArray;


    @BeforeClass
    public static void setUp() throws Exception{
        File f = new File(ClassLoader.getSystemClassLoader().getResource("example-response-without-coord.json").getFile());
        tweetWithoutCoord =  new String(Files.readAllBytes(f.toPath()));

        File ft = new File(ClassLoader.getSystemClassLoader().getResource("example-response-hashtag.json").getFile());
        tweetsArray = new String(Files.readAllBytes(ft.toPath()));
    }

    @Test
    public void generateCoordinates() throws Exception {

        String result = JsonHelper.generateCoordinates(tweetWithoutCoord);
        JSONObject tweetJson = new JSONObject(result);

        assertTrue(tweetJson.has(JsonHelper.KEY_COORDINATES));
        assertTrue(tweetJson.getJSONObject(JsonHelper.KEY_COORDINATES).has(JsonHelper.KEY_COORDINATES));

        assertFalse(tweetJson.isNull(JsonHelper.KEY_COORDINATES));
        assertFalse(tweetJson.has(JsonHelper.KEY_PLACE));
        assertTrue(tweetJson.getJSONObject(JsonHelper.KEY_COORDINATES).getJSONArray(JsonHelper.KEY_COORDINATES).getDouble(0) == 16.379845500000002);
        assertTrue(tweetJson.getJSONObject(JsonHelper.KEY_COORDINATES).getJSONArray(JsonHelper.KEY_COORDINATES).getDouble(1) == 48.220119999999994);
    }

    @Test
    public void extractTopTenHashtagsTest() throws Exception {
        JSONArray hashtags = JsonHelper.extractTopTenHashtags(tweetsArray);

        assertTrue(hashtags.length() == 10);

        assertTrue(Objects.equals(hashtags.getString(0), "#yolo"));
        assertTrue(Objects.equals(hashtags.getString(1), "#dirili"));
        System.out.println(hashtags.getString(2) + " <<-->> " + "#cosmoblogawards");
        assertTrue(Objects.equals(hashtags.getString(2), "#cosmoblogawards"));
    }

    @Test
    public void createTweetsWithHashtagRankingTest() throws Exception {
        String result = JsonHelper.createTweetsWithHashtagRanking(tweetsArray);
        JSONObject resultJsonObj = new JSONObject(result);

        assertTrue(resultJsonObj.has(JsonHelper.KEY_TWEETS));
        assertTrue(resultJsonObj.has(JsonHelper.KEY_TOPTEN));

        assertTrue(resultJsonObj.getJSONArray(JsonHelper.KEY_TOPTEN).length() == 10);
        assertTrue(Objects.equals(resultJsonObj.getJSONArray(JsonHelper.KEY_TOPTEN).getString(0), "#yolo"));
        assertTrue(Objects.equals(resultJsonObj.getJSONArray(JsonHelper.KEY_TOPTEN).getString(1), "#dirili"));

        assertTrue(resultJsonObj.getJSONArray(JsonHelper.KEY_TWEETS).length() == 34);

        JSONObject firstTweet = (JSONObject) resultJsonObj.getJSONArray(JsonHelper.KEY_TWEETS).get(0);
        assertTrue(firstTweet.has("text"));
        assertTrue(Objects.equals(firstTweet.getString("id_str"), "715193777833582592"));
    }

    @Test
    public void createTweetsWithoutHashtagRankingTest() throws Exception {
        String result = JsonHelper.createTweetsWithoutHashtagRanking(tweetsArray);
        JSONObject resultJsonObj = new JSONObject(result);

        assertTrue(resultJsonObj.has(JsonHelper.KEY_TWEETS));
        assertFalse(resultJsonObj.has(JsonHelper.KEY_TOPTEN));

        assertTrue(resultJsonObj.getJSONArray(JsonHelper.KEY_TWEETS).length() == 34);

        JSONObject firstTweet = (JSONObject) resultJsonObj.getJSONArray(JsonHelper.KEY_TWEETS).get(0);
        assertTrue(firstTweet.has("text"));
        assertTrue(Objects.equals(firstTweet.getString("id_str"), "715193777833582592"));
    }



}
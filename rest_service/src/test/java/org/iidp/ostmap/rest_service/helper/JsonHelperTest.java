package org.iidp.ostmap.rest_service.helper;

import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.*;

public class JsonHelperTest {

    @Test
    public void generateCoordinates() throws Exception {
        File f = new File(ClassLoader.getSystemClassLoader().getResource("example-response-without-coord.json").getFile());

        String tweet =  new String(Files.readAllBytes(f.toPath()));

        String result = JsonHelper.generateCoordinates(tweet);
        JSONObject tweetJson = new JSONObject(result);

        assertTrue(tweetJson.has("coordinates"));
        assertTrue(tweetJson.getJSONObject("coordinates").has("coordinates"));

        assertFalse(tweetJson.isNull("coordinates"));
        assertFalse(tweetJson.has("place"));
        assertTrue(tweetJson.getJSONObject("coordinates").getJSONArray("coordinates").getDouble(0) == 16.379845500000002);
        assertTrue(tweetJson.getJSONObject("coordinates").getJSONArray("coordinates").getDouble(1) == 48.220119999999994);
    }

}
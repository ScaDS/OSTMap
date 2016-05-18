package org.iidp.ostmap.commons.accumulo.geoTemp;

import com.github.davidmoten.geo.GeoHash;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.codec.Charsets;
import org.iidp.ostmap.commons.extractor.Extractor;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test-class for GeoTemporalKey
 */
public class GeoTemporalKeyTest {

    // hash function to use (murmurhash) see https://github.com/google/guava/wiki/HashingExplained
    private static HashFunction hashFunction = Hashing.murmur3_32();
    private static String tweet;
    private static short days;
    private static String geoHash;
    private static int spreadingByte;

    @BeforeClass
    public static void calcData() throws AccumuloException, AccumuloSecurityException, InterruptedException, IOException {
        tweet = "{\"created_at\":\"Fri Apr 29 09:05:55 +0000 2016\",\"id\":725974381906804738,\"id_str\":\"725974381906804738\",\"text\":\"Das funktioniert doch nie! #haselnuss\",\"source\":\"\\u003ca href=\\\"http:\\/\\/twitter.com\\/download\\/android\\\" rel=\\\"nofollow\\\"\\u003eTwitter for Android\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":179905182,\"id_str\":\"179905182\",\"name\":\"Peter Tosh\",\"screen_name\":\"PakistAnnie_\",\"location\":null,\"url\":null,\"description\":\"http:\\/\\/annietheorphan.tumblr.com \\/ We Gucci, You crocks \\/ the smell of expensive perfume and cheap tobacco \\/ I edit my bio a lot\",\"protected\":false,\"verified\":false,\"followers_count\":147,\"friends_count\":136,\"listed_count\":2,\"favourites_count\":926,\"statuses_count\":7002,\"created_at\":\"Wed Aug 18 11:16:51 +0000 2010\",\"utc_offset\":3600,\"time_zone\":\"Edinburgh\",\"geo_enabled\":true,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"131516\",\"profile_background_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_background_images\\/794919404\\/cf1ea49974012270a0f5eda0fdbc4c1b.jpeg\",\"profile_background_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_background_images\\/794919404\\/cf1ea49974012270a0f5eda0fdbc4c1b.jpeg\",\"profile_background_tile\":true,\"profile_link_color\":\"009999\",\"profile_sidebar_border_color\":\"000000\",\"profile_sidebar_fill_color\":\"EFEFEF\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/709493600745299969\\/LrE_LZYK_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/709493600745299969\\/LrE_LZYK_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/179905182\\/1460640737\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":{\"id\":\"1d73626cc863c69f\",\"url\":\"https:\\/\\/api.twitter.com\\/1.1\\/geo\\/id\\/1d73626cc863c69f.json\",\"place_type\":\"city\",\"name\":\"Riga\",\"full_name\":\"Riga, Latvia\",\"country_code\":\"LV\",\"country\":\"Latvija\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[23.932583,56.857067],[23.932583,57.085918],[24.324730,57.085918],[24.324730,56.857067]]]},\"attributes\":{}},\"contributors\":null,\"is_quote_status\":false,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[],\"urls\":[],\"user_mentions\":[],\"symbols\":[],\"media\":[{\"id\":725974374491295744,\"id_str\":\"725974374491295744\",\"indices\":[32,55],\"media_url\":\"http:\\/\\/pbs.twimg.com\\/media\\/ChMt2OOXEAArNJb.jpg\",\"media_url_https\":\"https:\\/\\/pbs.twimg.com\\/media\\/ChMt2OOXEAArNJb.jpg\",\"url\":\"https:\\/\\/t.co\\/LfhhspVMwj\",\"display_url\":\"pic.twitter.com\\/LfhhspVMwj\",\"expanded_url\":\"http:\\/\\/twitter.com\\/PakistAnnie_\\/status\\/725974381906804738\\/photo\\/1\",\"type\":\"photo\",\"sizes\":{\"small\":{\"w\":340,\"h\":453,\"resize\":\"fit\"},\"large\":{\"w\":720,\"h\":960,\"resize\":\"fit\"},\"medium\":{\"w\":600,\"h\":800,\"resize\":\"fit\"},\"thumb\":{\"w\":150,\"h\":150,\"resize\":\"crop\"}}}]},\"extended_entities\":{\"media\":[{\"id\":725974374491295744,\"id_str\":\"725974374491295744\",\"indices\":[32,55],\"media_url\":\"http:\\/\\/pbs.twimg.com\\/media\\/ChMt2OOXEAArNJb.jpg\",\"media_url_https\":\"https:\\/\\/pbs.twimg.com\\/media\\/ChMt2OOXEAArNJb.jpg\",\"url\":\"https:\\/\\/t.co\\/LfhhspVMwj\",\"display_url\":\"pic.twitter.com\\/LfhhspVMwj\",\"expanded_url\":\"http:\\/\\/twitter.com\\/PakistAnnie_\\/status\\/725974381906804738\\/photo\\/1\",\"type\":\"photo\",\"sizes\":{\"small\":{\"w\":340,\"h\":453,\"resize\":\"fit\"},\"large\":{\"w\":720,\"h\":960,\"resize\":\"fit\"},\"medium\":{\"w\":600,\"h\":800,\"resize\":\"fit\"},\"thumb\":{\"w\":150,\"h\":150,\"resize\":\"crop\"}}}]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"low\",\"lang\":\"lv\",\"timestamp_ms\":\"1461920755255\"}\n" ;
        days = 16920;
        int bufferSize = tweet.length();
        ByteBuffer bb = ByteBuffer.allocate(bufferSize);
        bb.put(tweet.getBytes(Charsets.UTF_8));
        spreadingByte = hashFunction.hashBytes(bb.array()).asInt() % 255;
        Double[] loc = Extractor.extractLocation(tweet);
        geoHash =  GeoHash.encodeHash(loc[1], loc[0], 8);
    }

    @Test
    public void testSomething() throws Exception {
        GeoTemporalKey geoTemporalKey = GeoTemporalKey.buildKey(tweet);

        // Check variables
        assertEquals(geoHash, geoTemporalKey.geoHash);
        assertTrue(spreadingByte == geoTemporalKey.spreadingByte);
        assertTrue(days == geoTemporalKey.day);

        // Check byteArrays
        assertEquals("56.971493/24.128656", GeoTemporalKey.columQualifierToString(geoTemporalKey.columQualifier));

        byte[] sliceDays = Arrays.copyOfRange(geoTemporalKey.rowBytes, 1, 3);
        short daysFromBA = ByteBuffer.wrap(sliceDays).getShort();
        assertTrue(daysFromBA==16920);

        byte[] subGeoHash = Arrays.copyOfRange(geoTemporalKey.rowBytes, 3, 10);
        String geoHashFromBA = new String(subGeoHash);
        assertEquals("ud1hj53", geoHashFromBA);

        int spreadlingByteFromBA = geoTemporalKey.rowBytes[0];
        assertTrue(spreadlingByteFromBA==108);
    }
}

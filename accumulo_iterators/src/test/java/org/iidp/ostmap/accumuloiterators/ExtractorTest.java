package org.iidp.ostmap.accumuloiterators;

import org.codehaus.jettison.json.JSONException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ExtractorTest {

    @Test
    public void testExtracor() throws JSONException {

        // without coordinates AND with place
        String original1= "{\"created_at\":\"Wed Mar 30 15:05:51 +0000 2016\",\"id\":715193326698364928,\"id_str\":\"715193326698364928\",\"text\":\"@Harrison_Andy very dark #BatmanvSuperman\",\"truncated\":false,\"in_reply_to_status_id\":713860098372845568,\"in_reply_to_status_id_str\":\"713860098372845568\",\"in_reply_to_user_id\":203852692,\"in_reply_to_user_id_str\":\"203852692\",\"in_reply_to_screen_name\":\"Harrison_Andy\",\"user\":{\"id\":703963,\"id_str\":\"703963\",\"name\":\"Philip Oakley\",\"screen_name\":\"philoakley\",\"location\":\"Stafford, Staffordshire, UK.\",\"url\":\"http://philipoakley.org\",\"description\":\"Xero Technologist, Cloud CRM and Social Media. Always happy to talk and connect. Instagram - http://goo.gl/G0ovHF LinkedIn - http://goo.gl/Zr2492\",\"protected\":false,\"verified\":false,\"followers_count\":4860,\"friends_count\":4647,\"listed_count\":284,\"favourites_count\":3096,\"statuses_count\":9026,\"created_at\":\"Thu Jan 25 21:38:37 +0000 2007\",\"utc_offset\":3600,\"time_zone\":\"London\",\"geo_enabled\":true,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"9AE4E8\",\"profile_background_image_url\":\"http://pbs.twimg.com/profile_background_images/255980590/twitter_background.jpg\",\"profile_background_image_url_https\":\"https://pbs.twimg.com/profile_background_images/255980590/twitter_background.jpg\",\"profile_background_tile\":true,\"profile_link_color\":\"0000FF\",\"profile_sidebar_border_color\":\"87BC44\",\"profile_sidebar_fill_color\":\"E0FF92\",\"profile_text_color\":\"000000\",\"profile_use_background_image\":true,\"profile_image_url\":\"http://pbs.twimg.com/profile_images/532930093266378752/8wtQek5n_normal.png\",\"profile_image_url_https\":\"https://pbs.twimg.com/profile_images/532930093266378752/8wtQek5n_normal.png\",\"profile_banner_url\":\"https://pbs.twimg.com/profile_banners/703963/1395523718\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":{\"id\":\"4f5e9c38e0de7f69\",\"url\":\"https://api.twitter.com/1.1/geo/id/4f5e9c38e0de7f69.json\",\"place_type\":\"city\",\"name\":\"Wednesfield\",\"full_name\":\"Wednesfield, England\",\"country_code\":\"GB\",\"country\":\"United Kingdom\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[-2.164786,52.546974],[-2.164786,52.637542],[-2.048029,52.637542],[-2.048029,52.546974]]]},\"attributes\":{}},\"contributors\":null,\"is_quote_status\":false,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[{\"text\":\"BatmanvSuperman\",\"indices\":[25,41]}],\"urls\":[],\"user_mentions\":[{\"screen_name\":\"Harrison_Andy\",\"name\":\"Andy Harrison\",\"id\":203852692,\"id_str\":\"203852692\",\"indices\":[0,14]}],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"filter_level\":\"low\",\"lang\":\"en\",\"timestamp_ms\":\"1459350351391\"}";

        // with coordinates AND without place
        String original2= "{\"created_at\":\"Wed Mar 30 15:05:51 +0000 2016\",\"id\":715193326698364928,\"id_str\":\"715193326698364928\",\"text\":\"@Harrison_Andy very dark #BatmanvSuperman\",\"truncated\":false,\"in_reply_to_status_id\":713860098372845568,\"in_reply_to_status_id_str\":\"713860098372845568\",\"in_reply_to_user_id\":203852692,\"in_reply_to_user_id_str\":\"203852692\",\"in_reply_to_screen_name\":\"Harrison_Andy\",\"user\":{\"id\":703963,\"id_str\":\"703963\",\"name\":\"Philip Oakley\",\"screen_name\":\"philoakley\",\"location\":\"Stafford, Staffordshire, UK.\",\"url\":\"http://philipoakley.org\",\"description\":\"Xero Technologist, Cloud CRM and Social Media. Always happy to talk and connect. Instagram - http://goo.gl/G0ovHF LinkedIn - http://goo.gl/Zr2492\",\"protected\":false,\"verified\":false,\"followers_count\":4860,\"friends_count\":4647,\"listed_count\":284,\"favourites_count\":3096,\"statuses_count\":9026,\"created_at\":\"Thu Jan 25 21:38:37 +0000 2007\",\"utc_offset\":3600,\"time_zone\":\"London\",\"geo_enabled\":true,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"9AE4E8\",\"profile_background_image_url\":\"http://pbs.twimg.com/profile_background_images/255980590/twitter_background.jpg\",\"profile_background_image_url_https\":\"https://pbs.twimg.com/profile_background_images/255980590/twitter_background.jpg\",\"profile_background_tile\":true,\"profile_link_color\":\"0000FF\",\"profile_sidebar_border_color\":\"87BC44\",\"profile_sidebar_fill_color\":\"E0FF92\",\"profile_text_color\":\"000000\",\"profile_use_background_image\":true,\"profile_image_url\":\"http://pbs.twimg.com/profile_images/532930093266378752/8wtQek5n_normal.png\",\"profile_image_url_https\":\"https://pbs.twimg.com/profile_images/532930093266378752/8wtQek5n_normal.png\",\"profile_banner_url\":\"https://pbs.twimg.com/profile_banners/703963/1395523718\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":{\"type\":\"Point\",\"coordinates\":[29.21240342,41.0087062]},\"place\":null,\"contributors\":null,\"is_quote_status\":false,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[{\"text\":\"BatmanvSuperman\",\"indices\":[25,41]}],\"urls\":[],\"user_mentions\":[{\"screen_name\":\"Harrison_Andy\",\"name\":\"Andy Harrison\",\"id\":203852692,\"id_str\":\"203852692\",\"indices\":[0,14]}],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"filter_level\":\"low\",\"lang\":\"en\",\"timestamp_ms\":\"1459350351391\"}";

        // without location and without coordinates
        String withoutBoth = "{\"created_at\":\"Wed Mar 30 15:05:51 +0000 2016\",\"id_str\":\"715193326698364928\",\"text\":\"@Harrison_Andy very dark #BatmanvSuperman\",\"user\":{\"name\":\"Philip Oakley\",\"screen_name\":\"philoakley\",\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/532930093266378752\\/8wtQek5n_normal.png\"}}";

        // text, user.name, user.screen_name,user.profile_image_url, coordinates, created_at, place.bounding_box.coordinates
        String expected = "{\"id_str\":\"715193326698364928\",\"created_at\":\"Wed Mar 30 15:05:51 +0000 2016\",\"text\":\"@Harrison_Andy very dark #BatmanvSuperman\",\"user\":{\"name\":\"Philip Oakley\",\"screen_name\":\"philoakley\",\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/532930093266378752\\/8wtQek5n_normal.png\"},\"coordinates\":null,\"place\":{\"bounding_box\":{\"coordinates\":[[[-2.164786,52.546974],[-2.164786,52.637542],[-2.048029,52.637542],[-2.048029,52.546974]]]}}}";

        ExtractIterator iter = new ExtractIterator();
        try {
            iter.init(null, null, null);
        } catch(IOException ioe) {
            System.out.println("ioe");
        }
        String reduced = iter.reduceJson(original1);
        Assert.assertEquals(expected, reduced);
        System.out.println("original json size: " + original1.length());
        System.out.println("reduced json size:  " + reduced.length());

        String withoutBothReduced = iter.reduceJson(withoutBoth);
        System.out.println(withoutBothReduced);

        String withCoordinatesReduces = iter.reduceJson(original2);
        System.out.println(withCoordinatesReduces);
        Assert.assertTrue(!withCoordinatesReduces.contains("point"));

    }
}

package org.iidp.ostmap.commons.extractor;

import org.junit.Assert;
import org.junit.Test;


public class ExtractorTest {
    public String json1= "{\n" +
            "    \"created_at\": \"Wed Mar 30 15:09:10 +0000 2016\",\n" +
            "    \"id\": 715194162648387584,\n" +
            "    \"id_str\": \"715194162648387584\",\n" +
            "    \"text\": \"\\ud83c\\udf88\\ud83c\\udf88 @ The Music Cafe https:\\/\\/t.co\\/OfO1dGNKEy\",\n" +
            "    \"source\": \"\\u003ca href=\\\"http:\\/\\/instagram.com\\\" rel=\\\"nofollow\\\"\\u003eInstagram\\u003c\\/a\\u003e\",\n" +
            "    \"truncated\": false,\n" +
            "    \"in_reply_to_status_id\": null,\n" +
            "    \"in_reply_to_status_id_str\": null,\n" +
            "    \"in_reply_to_user_id\": null,\n" +
            "    \"in_reply_to_user_id_str\": null,\n" +
            "    \"in_reply_to_screen_name\": null,\n" +
            "    \"user\": {\n" +
            "      \"id\": 949540494,\n" +
            "      \"id_str\": \"949540494\",\n" +
            "      \"name\": \"Bestetot.\",\n" +
            "      \"screen_name\": \"bestetot\",\n" +
            "      \"location\": null,\n" +
            "      \"url\": null,\n" +
            "      \"description\": \"rak\\u0131 deyince istemsiz g\\u00fcl\\u00fcms\\u00fcyorum, oluyo \\u00f6yle. EU\\/Biyoloji ~\",\n" +
            "      \"protected\": false,\n" +
            "      \"verified\": false,\n" +
            "      \"followers_count\": 482,\n" +
            "      \"friends_count\": 308,\n" +
            "      \"listed_count\": 1,\n" +
            "      \"favourites_count\": 2568,\n" +
            "      \"statuses_count\": 6355,\n" +
            "      \"created_at\": \"Thu Nov 15 11:22:35 +0000 2012\",\n" +
            "      \"utc_offset\": 10800,\n" +
            "      \"time_zone\": \"Athens\",\n" +
            "      \"geo_enabled\": true,\n" +
            "      \"lang\": \"tr\",\n" +
            "      \"contributors_enabled\": false,\n" +
            "      \"is_translator\": false,\n" +
            "      \"profile_background_color\": \"131516\",\n" +
            "      \"profile_background_image_url\": \"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme14\\/bg.gif\",\n" +
            "      \"profile_background_image_url_https\": \"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme14\\/bg.gif\",\n" +
            "      \"profile_background_tile\": true,\n" +
            "      \"profile_link_color\": \"470099\",\n" +
            "      \"profile_sidebar_border_color\": \"FFFFFF\",\n" +
            "      \"profile_sidebar_fill_color\": \"EFEFEF\",\n" +
            "      \"profile_text_color\": \"333333\",\n" +
            "      \"profile_use_background_image\": true,\n" +
            "      \"profile_image_url\": \"http:\\/\\/pbs.twimg.com\\/profile_images\\/697241339348590592\\/UNUhMfEJ_normal.jpg\",\n" +
            "      \"profile_image_url_https\": \"https:\\/\\/pbs.twimg.com\\/profile_images\\/697241339348590592\\/UNUhMfEJ_normal.jpg\",\n" +
            "      \"profile_banner_url\": \"https:\\/\\/pbs.twimg.com\\/profile_banners\\/949540494\\/1455069855\",\n" +
            "      \"default_profile\": false,\n" +
            "      \"default_profile_image\": false,\n" +
            "      \"following\": null,\n" +
            "      \"follow_request_sent\": null,\n" +
            "      \"notifications\": null\n" +
            "    },\n" +
            "    \"geo\": {\n" +
            "      \"type\": \"Point\",\n" +
            "      \"coordinates\": [\n" +
            "        38.4614716,\n" +
            "        27.2147884\n" +
            "      ]\n" +
            "    },\n" +
            "    \"coordinates\": {\n" +
            "      \"type\": \"Point\",\n" +
            "      \"coordinates\": [\n" +
            "        27.2147884,\n" +
            "        38.4614716\n" +
            "      ]\n" +
            "    },\n" +
            "    \"place\": {\n" +
            "      \"id\": \"0184147101a98fcf\",\n" +
            "      \"url\": \"https:\\/\\/api.twitter.com\\/1.1\\/geo\\/id\\/0184147101a98fcf.json\",\n" +
            "      \"place_type\": \"city\",\n" +
            "      \"name\": \"\\u0130zmir\",\n" +
            "      \"full_name\": \"\\u0130zmir, T\\u00fcrkiye\",\n" +
            "      \"country_code\": \"TR\",\n" +
            "      \"country\": \"T\\u00fcrkiye\",\n" +
            "      \"bounding_box\": {\n" +
            "        \"type\": \"Polygon\",\n" +
            "        \"coordinates\": [\n" +
            "          [\n" +
            "            [\n" +
            "              26.723167,\n" +
            "              38.283203\n" +
            "            ],\n" +
            "            [\n" +
            "              26.723167,\n" +
            "              38.532658\n" +
            "            ],\n" +
            "            [\n" +
            "              27.289855,\n" +
            "              38.532658\n" +
            "            ],\n" +
            "            [\n" +
            "              27.289855,\n" +
            "              38.283203\n" +
            "            ]\n" +
            "          ]\n" +
            "        ]\n" +
            "      },\n" +
            "      \"attributes\": {}\n" +
            "    },\n" +
            "    \"contributors\": null,\n" +
            "    \"is_quote_status\": false,\n" +
            "    \"retweet_count\": 0,\n" +
            "    \"favorite_count\": 0,\n" +
            "    \"entities\": {\n" +
            "      \"hashtags\": [],\n" +
            "      \"urls\": [\n" +
            "        {\n" +
            "          \"url\": \"https:\\/\\/t.co\\/OfO1dGNKEy\",\n" +
            "          \"expanded_url\": \"https:\\/\\/www.instagram.com\\/p\\/BDlOg90hrvS1Wu2th4tXM0ifAPhytlwFmq2nnU0\\/\",\n" +
            "          \"display_url\": \"instagram.com\\/p\\/BDlOg90hrvS1\\u2026\",\n" +
            "          \"indices\": [\n" +
            "            20,\n" +
            "            43\n" +
            "          ]\n" +
            "        }\n" +
            "      ],\n" +
            "      \"user_mentions\": [],\n" +
            "      \"symbols\": []\n" +
            "    },\n" +
            "    \"favorited\": false,\n" +
            "    \"retweeted\": false,\n" +
            "    \"possibly_sensitive\": false,\n" +
            "    \"filter_level\": \"low\",\n" +
            "    \"lang\": \"en\",\n" +
            "    \"timestamp_ms\": \"1459350550697\"\n" +
            "  }";

    public String json2= " {\n" +
            "    \"created_at\": \"Wed Mar 30 15:11:50 +0000 2016\",\n" +
            "    \"id\": 715194831379759104,\n" +
            "    \"id_str\": \"715194831379759104\",\n" +
            "    \"text\": \"jyuushimatsu is agender\",\n" +
            "    \"source\": \"\\u003ca href=\\\"http:\\/\\/twitter.com\\\" rel=\\\"nofollow\\\"\\u003eTwitter Web Client\\u003c\\/a\\u003e\",\n" +
            "    \"truncated\": false,\n" +
            "    \"in_reply_to_status_id\": null,\n" +
            "    \"in_reply_to_status_id_str\": null,\n" +
            "    \"in_reply_to_user_id\": null,\n" +
            "    \"in_reply_to_user_id_str\": null,\n" +
            "    \"in_reply_to_screen_name\": null,\n" +
            "    \"user\": {\n" +
            "      \"id\": 2590265922,\n" +
            "      \"id_str\": \"2590265922\",\n" +
            "      \"name\": \"the big gay\",\n" +
            "      \"screen_name\": \"yakisabo\",\n" +
            "      \"location\": \"austria\",\n" +
            "      \"url\": \"http:\\/\\/miyukki.flavors.me\",\n" +
            "      \"description\": \"Cass\\/23\\/genderfluid. @0nematsu is my battery partner \\u2764\\ufe0f \\/ @kylosolos is my bestest friend \\u2764\\ufe0f \\/ I love @yolozuyaaa very much \\u2764\\ufe0f \\/ @cham3l3ons is my qpprincess \\u2764\\ufe0f\",\n" +
            "      \"protected\": false,\n" +
            "      \"verified\": false,\n" +
            "      \"followers_count\": 171,\n" +
            "      \"friends_count\": 370,\n" +
            "      \"listed_count\": 0,\n" +
            "      \"favourites_count\": 48346,\n" +
            "      \"statuses_count\": 76210,\n" +
            "      \"created_at\": \"Thu Jun 26 21:22:55 +0000 2014\",\n" +
            "      \"utc_offset\": -25200,\n" +
            "      \"time_zone\": \"Pacific Time (US & Canada)\",\n" +
            "      \"geo_enabled\": true,\n" +
            "      \"lang\": \"en\",\n" +
            "      \"contributors_enabled\": false,\n" +
            "      \"is_translator\": false,\n" +
            "      \"profile_background_color\": \"C0DEED\",\n" +
            "      \"profile_background_image_url\": \"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\n" +
            "      \"profile_background_image_url_https\": \"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\n" +
            "      \"profile_background_tile\": false,\n" +
            "      \"profile_link_color\": \"EEEE00\",\n" +
            "      \"profile_sidebar_border_color\": \"C0DEED\",\n" +
            "      \"profile_sidebar_fill_color\": \"DDEEF6\",\n" +
            "      \"profile_text_color\": \"333333\",\n" +
            "      \"profile_use_background_image\": true,\n" +
            "      \"profile_image_url\": \"http:\\/\\/pbs.twimg.com\\/profile_images\\/715042999508537349\\/yfxgBEBs_normal.jpg\",\n" +
            "      \"profile_image_url_https\": \"https:\\/\\/pbs.twimg.com\\/profile_images\\/715042999508537349\\/yfxgBEBs_normal.jpg\",\n" +
            "      \"profile_banner_url\": \"https:\\/\\/pbs.twimg.com\\/profile_banners\\/2590265922\\/1459138490\",\n" +
            "      \"default_profile\": false,\n" +
            "      \"default_profile_image\": false,\n" +
            "      \"following\": null,\n" +
            "      \"follow_request_sent\": null,\n" +
            "      \"notifications\": null\n" +
            "    },\n" +
            "    \"geo\": null,\n" +
            "    \"coordinates\": null,\n" +
            "    \"place\": {\n" +
            "      \"id\": \"9f659d51e5c5deae\",\n" +
            "      \"url\": \"https:\\/\\/api.twitter.com\\/1.1\\/geo\\/id\\/9f659d51e5c5deae.json\",\n" +
            "      \"place_type\": \"city\",\n" +
            "      \"name\": \"Vienna\",\n" +
            "      \"full_name\": \"Vienna, Austria\",\n" +
            "      \"country_code\": \"AT\",\n" +
            "      \"country\": \"\\u00d6sterreich\",\n" +
            "      \"bounding_box\": {\n" +
            "        \"type\": \"Polygon\",\n" +
            "        \"coordinates\": [\n" +
            "          [\n" +
            "            [\n" +
            "              16.182180,\n" +
            "              48.117666\n" +
            "            ],\n" +
            "            [\n" +
            "              16.182180,\n" +
            "              48.322574\n" +
            "            ],\n" +
            "            [\n" +
            "              16.577511,\n" +
            "              48.322574\n" +
            "            ],\n" +
            "            [\n" +
            "              16.577511,\n" +
            "              48.117666\n" +
            "            ]\n" +
            "          ]\n" +
            "        ]\n" +
            "      },\n" +
            "      \"attributes\": {}\n" +
            "    },\n" +
            "    \"contributors\": null,\n" +
            "    \"is_quote_status\": false,\n" +
            "    \"retweet_count\": 0,\n" +
            "    \"favorite_count\": 0,\n" +
            "    \"entities\": {\n" +
            "      \"hashtags\": [],\n" +
            "      \"urls\": [],\n" +
            "      \"user_mentions\": [],\n" +
            "      \"symbols\": []\n" +
            "    },\n" +
            "    \"favorited\": false,\n" +
            "    \"retweeted\": false,\n" +
            "    \"filter_level\": \"low\",\n" +
            "    \"lang\": \"in\",\n" +
            "    \"timestamp_ms\": \"1459350710135\"\n" +
            "  }";
    @Test
    public void testextractLocations(){

        Double[] extract1= Extractor.extractLocation(json1);
        Double[] extract2= Extractor.extractLocation(json2);

        System.out.println(extract1[0]+"  " + extract1[1]);

        Assert.assertTrue(38.4614716==extract1[1]);
        Assert.assertTrue(27.2147884==extract1[0]);


        Double testLong = (16.182180+16.182180+16.577511+16.577511)/4;
        Double testLat = ( 48.117666+48.322574+48.322574+48.117666)/4;

        System.out.println("ext2[0]: "+ extract2[0] + " testLong: "+ testLong);
        System.out.println("ext2[1]: "+ extract2[1] + " testLat: "+ testLat);


        Assert.assertTrue(testLong.equals(extract2[0]));
        Assert.assertTrue(testLat.equals(extract2[1]));
    }
}


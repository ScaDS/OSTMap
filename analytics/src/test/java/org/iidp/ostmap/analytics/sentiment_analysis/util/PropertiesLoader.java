package org.iidp.ostmap.analytics.sentiment_analysis.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class PropertiesLoader {

    private Config config = ConfigFactory.load("application.conf");
    public String oAuthConsumerKey;
    public String oAuthConsumerSecret;
    public String oAuthAccessToken;
    public String oAuthAccessTokenSecret;

    public PropertiesLoader() {
        this.oAuthConsumerKey = this.config.getString("CONSUMER_KEY");
        this.oAuthConsumerSecret = this.config.getString("CONSUMER_SECRET");
        this.oAuthAccessToken = this.config.getString("ACCESS_TOKEN");
        this.oAuthAccessTokenSecret = this.config.getString("ACCESS_TOKEN_SECRET");
    }
}
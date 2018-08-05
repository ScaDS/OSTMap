package org.iidp.ostmap.analytics.sentiment_analysis.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class PropertiesLoaderJ {

    private Config config = ConfigFactory.load("application.conf");
    public String oAuthConsumerKey;
    public String oAuthConsumerSecret;
    public String oAuthAccessToken;
    public String oAuthAccessTokenSecret;
    public String naiveBayesModelPath;
    public String stopwordsPath;
    public String tweetsClassifiedPath;

    public PropertiesLoaderJ() {
        this.oAuthConsumerKey = this.config.getString("CONSUMER_KEY");
        this.oAuthConsumerSecret = this.config.getString("CONSUMER_SECRET");
        this.oAuthAccessToken = this.config.getString("ACCESS_TOKEN");
        this.oAuthAccessTokenSecret = this.config.getString("ACCESS_TOKEN_SECRET");

//        this.naiveBayesModelPath = this.config.getString("NAIVE_BAYES_MODEL_PATH");
//        this.stopwordsPath = this.config.getString("STOPWORDS_PATH");
//        this.tweetsClassifiedPath = this.config.getString("TWEETS_CLASSIFIED_PATH");
    }
}
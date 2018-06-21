package org.iidp.ostmap.analytics.sentiment_analysis.util;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.List;


/**
 * tbd.
 */
public class TwitterConnector {

    public TwitterConnector() {
    }

    /**
     * Connects to Twitter using twitter4j to retrieve all tweets from ScaDS (BigData Center).
     * It returns a list of statuses.
     * @return list of statuses
     * @throws TwitterException
     */
    public static List<Status> getScadsTweets() throws TwitterException {
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

        // Loads authentication data for twitter access from ./resources/application.conf (not in the Git repo)
        PropertiesLoader propertiesLoader = new PropertiesLoader();

        configurationBuilder.setOAuthConsumerKey(propertiesLoader.oAuthConsumerKey)
                .setOAuthConsumerSecret(propertiesLoader.oAuthConsumerSecret)
                .setOAuthAccessToken(propertiesLoader.oAuthAccessToken)
                .setOAuthAccessTokenSecret(propertiesLoader.oAuthAccessTokenSecret);

        Twitter twitter = new TwitterFactory(configurationBuilder.build()).getInstance();
        Paging paging = new Paging(1, 300);
        List<Status> statusList = twitter.getUserTimeline("Sca_DS", paging);

        return statusList;
    }
}

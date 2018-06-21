package org.iidp.ostmap.analytics.sentiment_analysis;
import org.apache.accumulo.core.client.Connector;
import org.iidp.ostmap.analytics.sentiment_analysis.stanford.StanfordSentimentAnalyzer;
import org.iidp.ostmap.analytics.sentiment_analysis.util.PropertiesLoader;
import org.iidp.ostmap.analytics.sentiment_analysis.util.TwitterConnector;
import org.iidp.ostmap.commons.accumulo.AmcHelper;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * tbd...
 */
public class SentimentAnalysisTest {

    /**
     * Directory for MiniAccumuloCluster data.
     */
    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();

    /**
     * Directory for MiniAccumuloCluster settings.
     */
    public static TemporaryFolder tmpSettingsDir = new TemporaryFolder();

    /**
     * Helper class which provides a MiniAccumuloCluster as well as a Connector.
     */
    public static AmcHelper amc = new AmcHelper();

    /**
     * Empty constructor.
     */
    public SentimentAnalysisTest() {
    }

    /**
     * Starts the MiniAccumuloCluster. Required before running tests.
     */
    @BeforeClass
    public static void setUpCluster(){
        amc.startMiniCluster(tmpDir.getRoot().getAbsolutePath());
    }

    /**
     * Shuts down the MiniAccumuloCluster after running tests.
     */
    @AfterClass
    public static void shutDownCluster(){
        amc.stopMiniCluster();
    }

    /**
     * tbd...
     */
    @Test
    public void testFetchRawTwitterData() throws IOException, TwitterException {
        Connector conn = amc.getConnector();
        System.out.println("I am connected as: " + conn.whoami());

        List<Status> statusList = TwitterConnector.getScadsTweets();

        StanfordSentimentAnalyzer stanfordSentimentAnalyzer = new StanfordSentimentAnalyzer();
        Map<Long, HashMap> predictedTweetSentiments = stanfordSentimentAnalyzer.predictTweetSentiments(statusList);
        System.out.println();
    }


    /**
     * Prints out tweet-id and tweet itself of each status from the given list.
     * @param statusList list of twitter statuses
     */
    private void printStatusList(List<Status> statusList){
        for (Status status: statusList) {
            System.out.println(String.format("[TWEET-ID] %s [TWEET] %s", status.getId(), status.getText()));
        }
    }
}

package org.iidp.ostmap.analytics.sentiment_analysis;
import org.apache.accumulo.core.client.Connector;
import org.iidp.ostmap.commons.accumulo.AmcHelper;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

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
    public void testFetchRawTwitterData() throws IOException {
        Connector conn = amc.getConnector();
        System.out.println("I am connected as: " + conn.whoami());
        List<String> sentenceList = this.getTwitterData();

        for (String sentence : sentenceList) {
            System.out.println("sentence = " + sentence);
        }
    }

    /**
     * Reads Twitter data from a previously create text file, which contains twitter sentences.
     * @return List of sentences.
     */
    private List<String> getTwitterData() throws IOException {
        List<String> sentenceList = new ArrayList<>();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("/home/alex/coreNLPExample/data/results/stanford")));

        String currentLine;

        while ((currentLine = bufferedReader.readLine()) != null) {
		    sentenceList.add(currentLine);
        }

        return sentenceList;
    }
}

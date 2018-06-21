package org.iidp.ostmap.analytics.sentiment_analysis;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.iidp.ostmap.analytics.sentiment_analysis.util.TwitterConnector;
import org.junit.Test;
import twitter4j.Status;

import java.util.List;

public class FlinkTwitterTest {

    public FlinkTwitterTest(){

    }

    @Test
    public void testBatchProcessing() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Status> scadsTweets = TwitterConnector.getScadsTweets();

        env.execute("Flink Batch Processing Tweets");

    }
}

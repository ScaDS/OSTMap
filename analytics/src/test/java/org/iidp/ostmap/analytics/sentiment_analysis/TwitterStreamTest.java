package org.iidp.ostmap.analytics.sentiment_analysis;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.iidp.ostmap.analytics.sentiment_analysis.util.PropertiesLoader;
import org.iidp.ostmap.commons.accumulo.AmcHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TwitterStreamTest {

    private static final List<String> TagArray = new ArrayList<String>(Arrays.asList("NASA", "Discovery", "Interstellar"));

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
     * Empty Constructor
     */
    public TwitterStreamTest() {
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

    @Test
    public void testTwitterStreaming(){

        // create streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        // enable event time processing
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        env.getConfig().setAutoWatermarkInterval(1000L);
//        env.setParallelism(1);
//
//        // enable fault-tolerance, 60s checkpointing
//        env.enableCheckpointing(60000);
//
//        // enable restarts
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(0, 500L));

        PropertiesLoader propertiesLoader = new PropertiesLoader();

        Properties props = new Properties();

        props.setProperty(TwitterSource.CONSUMER_KEY, propertiesLoader.oAuthConsumerKey);
        props.setProperty(TwitterSource.CONSUMER_SECRET, propertiesLoader.oAuthConsumerSecret);
        props.setProperty(TwitterSource.TOKEN, propertiesLoader.oAuthAccessToken);
        props.setProperty(TwitterSource.TOKEN_SECRET, propertiesLoader.oAuthAccessTokenSecret);

        TwitterSource twitterSource = new TwitterSource(props);
        TweetFilter customFilterInitializer = new TweetFilter();
        twitterSource.setCustomEndpointInitializer(customFilterInitializer);

        DataStream<String> streamSource = env.addSource(new TwitterSource(props));
    }

    public static class TweetFilter implements TwitterSource.EndpointInitializer, Serializable {
        @Override
        public StreamingEndpoint createEndpoint() {
            StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
            endpoint.trackTerms(TagArray);
            return endpoint;
        }
    }
}

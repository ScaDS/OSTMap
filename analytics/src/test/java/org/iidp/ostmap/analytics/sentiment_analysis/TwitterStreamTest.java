package org.iidp.ostmap.analytics.sentiment_analysis;

import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.iidp.ostmap.analytics.sentiment_analysis.util.PropertiesLoader;
import org.iidp.ostmap.analytics.sentiment_analysis.util.TwitterConnector;
import org.iidp.ostmap.commons.accumulo.AmcHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import twitter4j.Status;
import twitter4j.TwitterException;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

//import org.apache.flink.streaming.examples.twitter.util.TwitterExampleData;

public class TwitterStreamTest {

    private static final Config config = ConfigFactory.load("application.conf");

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
    public static void setUpCluster() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, TwitterException {
        amc.startMiniCluster(tmpDir.getRoot().getAbsolutePath());

        Connector conn = amc.getConnector();
        System.out.println("I am connected as: " + conn.whoami());

        // setup accumulo tables
        conn.tableOperations().create("Sentiment");

//        // retrieve tweets from ScaDS
//        // todo --> rawTweet tab
        List<Status> statusList = TwitterConnector.getScadsTweets();

        //
        List<Mutation> mutations = new ArrayList<>();
        Mutation mutation = null;

        SimpleDateFormat sdf = new SimpleDateFormat("dd");
        Random random = new Random();
        int spreadingByte = 0;
        ObjectMapper jsonParser = new ObjectMapper();

        for (Status status : statusList) {
            String stamp = sdf.format(new Timestamp(status.getCreatedAt().getTime()));

//            JsonNode jsonNode = (JsonNode) status;

//            boolean hasGeo = jsonNode.has("geo");


            // rowKey --> spreadingByte,day,geohash
            // example: rowKey = 57:15:GeoLocation{latitude=51.33902167, longitude=12.37988333}
            spreadingByte = random.nextInt(256);
            String rowKey = String.format("%d:%s:%s", spreadingByte, stamp, status.getGeoLocation());
            System.out.println("rowKey = " + rowKey);
            mutation = new Mutation(rowKey);
            mutation.put("CF", "CQ", status.getText());
            mutations.add(mutation);


        }

        BatchWriter batchWriter = conn.createBatchWriter("Sentiment", new BatchWriterConfig());
        batchWriter.addMutations(mutations);

    }

    /**
     * Shuts down the MiniAccumuloCluster after running tests.
     */
    @AfterClass
    public static void shutDownCluster(){
        amc.stopMiniCluster();
    }

    @Test
    public void testTwitterStreaming() throws Exception {

        // create streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();

        props.setProperty(TwitterSource.CONSUMER_KEY, config.getString("CONSUMER_KEY"));
        props.setProperty(TwitterSource.CONSUMER_SECRET, config.getString("CONSUMER_SECRET"));
        props.setProperty(TwitterSource.TOKEN, config.getString("ACCESS_TOKEN"));
        props.setProperty(TwitterSource.TOKEN_SECRET, config.getString("ACCESS_TOKEN_SECRET"));

        DataStream<String> streamSource = env.addSource(new TwitterSource(props));
        DataStream<Tuple2<String, Integer>> tweets = streamSource
                .flatMap(new StanfordSentimentFlatMap())
                .keyBy(0);
        tweets.print();
        streamSource.print();
        env.execute("twitter stream connection test");
    }

    public static class StanfordSentimentFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper jsonParser;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

            boolean hasText = jsonNode.has("text");

            if (hasText) {

                // set up pipeline properties
                Properties pipelineProperties = new Properties();

                // set the list of annotators to run
                pipelineProperties.setProperty("annotators", "tokenize,ssplit,pos,lemma,parse,sentiment");

                // build pipeline
                StanfordCoreNLP pipeline = new StanfordCoreNLP(pipelineProperties);

                Annotation document;

                // create an empty annotation just with the given text
                document = new Annotation(jsonNode.get("text").asText());

                // annnotate the document
                pipeline.annotate(document);

                // these are all the sentences in this document
                // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
                List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

                Map<Annotation, Integer> sentenceMap = new HashMap<>();

                for (CoreMap sentence : sentences) {

                    // this is the parse tree of the current sentence
                    Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);

                    /**
                     * RNNCoreAnnotations.getPredictedClass
                     * 0 = very negative
                     * 1 = negative
                     * 2 = neutral
                     * 3 = positive
                     * 4 = very positive
                     */
                    Integer sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                    sentenceMap.put((Annotation) sentence, sentiment);
                    System.out.println(String.format("[SENTIMENT] %s - [SENTENCE] %s", sentiment.toString(), sentence));

                    if (!sentence.toString().equals("")) {
                        out.collect(new Tuple2<>(sentence.toString(), sentiment));
                    }
                }
            }
        }
    }
}

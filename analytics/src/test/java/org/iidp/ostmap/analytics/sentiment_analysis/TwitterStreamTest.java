package org.iidp.ostmap.analytics.sentiment_analysis;

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
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.Text;
import org.iidp.ostmap.analytics.sentiment_analysis.util.TwitterConnector;
import org.iidp.ostmap.commons.accumulo.AmcHelper;
import org.iidp.ostmap.commons.enums.AccumuloIdentifiers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import twitter4j.Status;
import twitter4j.TwitterException;

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
     * Helper class which provides a MiniAccumuloCluster as well as a Connector.
     */
    private static AmcHelper amc = new AmcHelper();

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
        System.out.println("[WHOAMI]" + conn.whoami());

        if(!conn.tableOperations().exists("SentimentData")){
            conn.tableOperations().create("SentimentData");
        }

        // retrieve tweets from ScaDS
        List<Status> statusList = TwitterConnector.getScadsTweets();

        List<Mutation> mutations = new ArrayList<>();
        Mutation mutation;

        SimpleDateFormat sdf = new SimpleDateFormat("dd");
        Random random = new Random();
        int spreadingByte = 0;

        Double longitude = 0.0;
        Double latitude = 0.0;
        String rowKey;
        String columnFamily;

        for (Status status : statusList) {
            String stamp = sdf.format(new Timestamp(status.getCreatedAt().getTime()));
            spreadingByte = random.nextInt(256);

            if (status.getGeoLocation() != null){
                longitude = status.getGeoLocation().getLongitude();
                latitude = status.getGeoLocation().getLatitude();
                columnFamily = "geo";

            } else {
                columnFamily = "no_geo";
            }

            rowKey = String.format("%d:%s:%f:%f", spreadingByte, stamp, longitude, latitude);
            mutation = new Mutation(rowKey);
            mutation.put(columnFamily, "CQ", status.getText());

            mutations.add(mutation);
        }
//        System.out.println(String.format("[SENTIMENT_DATA] added %d lines to table: SENTIMENT_DATA.", mutations.size()));
        BatchWriter batchWriter = conn.createBatchWriter("SentimentData", new BatchWriterConfig());
        batchWriter.addMutations(mutations);
        batchWriter.close();
    }

    /**
     * Shuts down the MiniAccumuloCluster after running tests.
     */
    @AfterClass
    public static void shutDownCluster(){
        amc.stopMiniCluster();
    }

    /**
     * This test simulates an analysis batch job from a possible user input form at the OSTMap web application.
     * @throws TableNotFoundException Exception thrown if Table not found.
     */
    @Test
    public void testTweetsLastNDays() throws TableNotFoundException {
        int days = 5;
        Connector connector = amc.getConnector();
        Authorizations authorizations = new Authorizations(AccumuloIdentifiers.AUTHORIZATION.toString());
        System.out.println(String.format("[AUTHORIZATIONS] %s", authorizations.toString()));
        Scanner scanner = connector.createScanner("SentimentData", authorizations);
        scanner.fetchColumn(new Text("geo"), new Text("CQ"));
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.printf("Key : %-50s  Value : %s\n", entry.getKey(), entry.getValue());
        }
        // tbd ...
    }


//    @Test
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

package org.iidp.ostmap.analytics.sentiment_analysis;

import com.github.davidmoten.geo.GeoHash;
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
import org.apache.log4j.Logger;
import org.iidp.ostmap.commons.accumulo.AmcHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.TwitterException;

import java.text.SimpleDateFormat;
import java.util.*;

//import org.apache.flink.streaming.examples.twitter.util.TwitterExampleData;

public class StreamingTest {

    private static final Config config = ConfigFactory.load("application.conf");
    private static final Logger log = Logger.getLogger(StreamingTest.class);

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
    public StreamingTest() {
    }

    /**
     * Starts the MiniAccumuloCluster. Required before running tests.
     */
    @BeforeClass
    public static void setUpCluster() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, TwitterException {
        amc.startMiniCluster(tmpDir.getRoot().getAbsolutePath());

//        Connector connector = amc.getConnector();
//        System.out.println("I am connected as: " + connector.whoami());

//        Authorizations authorizations = new Authorizations(AccumuloIdentifiers.AUTHORIZATION.toString());

        // retrieve tweets from ScaDS
//        List<Status> statusList = TwitterConnector.getTweetsFromUser("Sca_DS");

        // create GeoTemporalIndex
//        StreamingTest.createTableGeoTemporalIndex(connector, statusList);

    }

    /**
     * Shuts down the MiniAccumuloCluster after running tests.
     */
    @AfterClass
    public static void shutDownCluster(){
        amc.stopMiniCluster();
    }

    @Test
    public void testPrintHallo(){
        System.out.println("Hallo Welt!");
    }

    @Test
    public void testTwitterStreaming() throws Exception {

        // create streaming environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();

        props.setProperty(TwitterSource.CONSUMER_KEY, config.getString("CONSUMER_KEY"));
        props.setProperty(TwitterSource.CONSUMER_SECRET, config.getString("CONSUMER_SECRET"));
        props.setProperty(TwitterSource.TOKEN, config.getString("ACCESS_TOKEN"));
        props.setProperty(TwitterSource.TOKEN_SECRET, config.getString("ACCESS_TOKEN_SECRET"));

        DataStream<String> streamSource = env.addSource(new TwitterSource(props));
//        DataStream<Tuple2<String, Integer>> tweets = streamSource
//                .flatMap(new StanfordSentimentFlatMap())
//                .keyBy(0);
////        tweets.print();
//        env.execute("StreamProcessing TestCase");
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

    private static void createTableGeoTemporalIndex(Connector connector, List<Status> statusList)
            throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {

        if(!connector.tableOperations().exists("GeoTemporalIndex")){
            connector.tableOperations().create("GeoTemporalIndex");
        }

        List<Mutation> mutations = new ArrayList<>();
        Mutation mutation;

        SimpleDateFormat sdf = new SimpleDateFormat("dd");
        Random random = new Random();
        int spreadingByte = 0;

        // longitude, latitude
        Double tweetCoordinates[] = {0.0, 0.0};
        String rowKey;
        String columnFamily = "";
        String columnQualifier = "";
        String day;
        String geohash;

        for (Status status : statusList) {
            // reset tweet coordinates before each iteration step
            tweetCoordinates[0] = 0.0;
            tweetCoordinates[1] = 0.0;

//            day = sdf.format(new Timestamp(status.getCreatedAt().getTime()));
            day = status.getCreatedAt().toString();
            spreadingByte = random.nextInt(256);

            if (status.getPlace() != null){
                // Pseudo-RawTweetKey
                columnFamily = String.valueOf(status.getId());
                tweetCoordinates = StreamingTest.getCoordinates(status);
                geohash = StreamingTest.getGeoHash(tweetCoordinates);
                rowKey = String.format("%d:%s:%s", spreadingByte, day, geohash);
                columnQualifier = String.format("%s/%s", tweetCoordinates[1], tweetCoordinates[0]);

                mutation = new Mutation(rowKey);
                mutation.put(columnFamily, columnQualifier, status.toString());
                mutations.add(mutation);
            }
        }
        BatchWriter batchWriter = connector.createBatchWriter("GeoTemporalIndex", new BatchWriterConfig());
        batchWriter.addMutations(mutations);
        batchWriter.close();
    }
    private static Double[] getCoordinates(Status status){
        Double tweetCoordinates[] = {0.0, 0.0};
        GeoLocation boundingBox[][];

        if (status.getPlace() != null) {
            boundingBox = status.getPlace().getBoundingBoxCoordinates();

            // longitude
            tweetCoordinates[0] = (boundingBox[0][0].getLongitude() +
                    boundingBox[0][1].getLongitude() +
                    boundingBox[0][2].getLongitude() +
                    boundingBox[0][3].getLongitude()) / 4;

            // latitude
            tweetCoordinates[1] = (boundingBox[0][0].getLatitude() +
                    boundingBox[0][1].getLatitude() +
                    boundingBox[0][2].getLatitude() +
                    boundingBox[0][3].getLatitude()) / 4;
        }

        return tweetCoordinates;
    }
    private static String getGeoHash(Double[] coordinates){
        return GeoHash.encodeHash(coordinates[1], coordinates[0]);
    }
}

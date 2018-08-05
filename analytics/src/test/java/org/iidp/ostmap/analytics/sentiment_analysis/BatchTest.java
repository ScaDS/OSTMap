/*
 * This test file, bla bla --> tbd...
 */

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
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import org.iidp.ostmap.analytics.sentiment_analysis.util.TwitterConnector;
import org.iidp.ostmap.commons.accumulo.AmcHelper;
import org.iidp.ostmap.commons.accumulo.FlinkEnvManager;
import org.iidp.ostmap.commons.enums.AccumuloIdentifiers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.TwitterException;

import java.io.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

//import org.apache.flink.streaming.examples.twitter.util.TwitterExampleData;

public class BatchTest {

    private static final Config config = ConfigFactory.load("application.conf");

    /**
     * Directory for MiniAccumuloCluster data.
     */
    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();

    private static TemporaryFolder tmpSettingsDir = new TemporaryFolder();

    /**
     * Helper class which provides a MiniAccumuloCluster as well as a Connector.
     */
    private static AmcHelper amc = new AmcHelper();

    /**
     * Empty Constructor
     */
    public BatchTest() {
    }

    /**
     * Starts the MiniAccumuloCluster. Required before running tests.
     */
    @BeforeClass
    public static void setUpCluster() throws Exception {
        amc.startMiniCluster(tmpDir.getRoot().getAbsolutePath());

        Connector connector = amc.getConnector();
        System.out.println(String.format("[CONNECTOR] I am \'%s\'.", connector.whoami()));

        Authorizations authorizations = new Authorizations(AccumuloIdentifiers.AUTHORIZATION.toString());

        // retrieve tweets from ScaDS
        List<Status> statusList = TwitterConnector.getTweetsFromUser("Sca_DS");

        // create GeoTemporalIndex
        BatchTest.createTableGeoTemporalIndex(connector, statusList);

        // create sentiment table
        BatchTest.createTableSentimentData(connector);

        // compute sentiments
        BatchTest.computeSentiments(connector, authorizations);

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
//    @Test
    public void testTweetsLastNDays() throws TableNotFoundException {
        int days = 5;
        Connector connector = amc.getConnector();
        Authorizations authorizations = new Authorizations(AccumuloIdentifiers.AUTHORIZATION.toString());
        System.out.println(String.format("[AUTHORIZATIONS] %s", authorizations.toString()));
        Scanner scanner = connector.createScanner("GeoTemporalIndex", authorizations);

        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.printf("Key : %-50s  Value : %s\n", entry.getKey(), entry.getValue());
        }
        // tbd ...
    }

    /**
     * tbd ...
     */
    public static class StanfordSentimentFlatMap implements FlatMapFunction<Tuple2<Key, Value>, Tuple3<String, Integer, String>> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper jsonParser;

        Connector connector = amc.getConnector();
        Authorizations authorizations = new Authorizations(AccumuloIdentifiers.AUTHORIZATION.toString());
        Scanner scanner = connector.createScanner("GeoTemporalIndex", authorizations);

        List<Mutation> mutations = new ArrayList<>();
        Mutation mutation;

        public StanfordSentimentFlatMap() throws TableNotFoundException {
        }

        @Override
        public void flatMap(Tuple2<Key, Value> in, Collector<Tuple3<String, Integer, String>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(in.f1.toString(), JsonNode.class);

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

//                Map<Annotation, Integer> sentenceMap = new HashMap<>();

                int sumOfSentiments = 0;
                int sentiment;
                String sentimentPolarity = "";

                for (CoreMap sentence : sentences) {
                    // this is the parse tree of the current sentence
                    Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                    sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                    sumOfSentiments += sentiment;

                }
                int avgSentiment = sumOfSentiments / sentences.size();
                System.out.println("avgSentiment = " + avgSentiment);
                sentimentPolarity = BatchTest.getSentimentPolarity(avgSentiment);
                out.collect(new Tuple3<>(jsonNode.get("id").asText(), avgSentiment, sentimentPolarity));



            }
        }
    }

    /**
     * Returns the coordinates of a geo-tagged tweet from a bounding box.
     * Since the bounding box has four pairs of coordinates,
     * the average of the longitude and latitude is calculated from each of
     * these to obtain the coordinates of the center of the box.
     * @param status This should be a Twitter4J.Status object.
     * @return Double[], with first element longitude and second latitude of the tweet.
     */
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

    /**
     * Represents the GeoTemporalIndex.
     * +----------------+----------+---------+-----------------------------+
     * |       ROW      |    CF    |    CQ   |        VALUE                |
     * +----------------+----------+---------+-----------------------------+
     * | sb,day,geohash | Tweet-ID | lat/lon | Twitter4J.Status.toString() |
     * +----------------+----------+---------+-----------------------------+
     *
     * @param connector Connector connects to an Accumulo instance to perform table operations.
     * @param statusList List of multiple Twitter4J.Status instances.
     * @throws TableExistsException Thrown when the table specified already exists,
     * and it was expected that it didn't.
     * @throws AccumuloSecurityException An Accumulo Exception for security violations,
     * authentication failures, authorization failures, etc.
     * @throws AccumuloException A generic Accumulo Exception for general accumulo failures.
     * @throws TableNotFoundException Thrown when the table specified doesn't exist when it was expected to.
     */
    private static void createTableGeoTemporalIndex(Connector connector, List<Status> statusList)
            throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {

        if(!connector.tableOperations().exists("GeoTemporalIndex")){
            connector.tableOperations().create("GeoTemporalIndex");
        }

        List<Mutation> mutations = new ArrayList<>();
        Mutation mutation;

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

            day = status.getCreatedAt().toString();
            spreadingByte = random.nextInt(256);

            if (status.getPlace() != null){
                // Pseudo-RawTweetKey
                columnFamily = String.valueOf(status.getId());
                tweetCoordinates = BatchTest.getCoordinates(status);
                geohash = BatchTest.getGeoHash(tweetCoordinates);
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

    /**
     * Creates the table for tweet sentiments.
     * +----------------+----------+---------+-----------+
     * |       ROW      |    CF    |    CQ   |   VALUE   |
     * +----------------+----------+---------+-----------+
     * | sb,day,geohash | polarity |         | Sentiment |
     * +----------------+----------+---------+-----------+
     *
     * @param connector Connector connects to an Accumulo instance to perform table operations.
     * @throws TableExistsException Thrown when the table specified already exists,
     * and it was expected that it didn't.
     * @throws AccumuloSecurityException An Accumulo Exception for security violations,
     * authentication failures, authorization failures, etc.
     * @throws AccumuloException A generic Accumulo Exception for general accumulo failures.
     */
    private static void createTableSentimentData(Connector connector)
            throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {

        if(!connector.tableOperations().exists("SentimentData")){
            connector.tableOperations().create("SentimentData");
        }

    }

    /**
     * Computes a geo-hash by given longitude and latitude.
     * @param coordinates Array of two doubles representing latitude and longitude.
     * @return Geo-Hash String
     */
    private static String getGeoHash(Double[] coordinates){
        return GeoHash.encodeHash(coordinates[1], coordinates[0]);
    }

    /**
     * tbd ...
     * @param connector Connector connects to an Accumulo instance to perform table operations.
     * @param authorizations A collection of authorization strings.
     * @throws TableNotFoundException Thrown when the table specified doesn't exist when it was expected to.
     */
    private static void computeSentiments(Connector connector, Authorizations authorizations)
            throws Exception {

        File settings = BatchTest.createSettingsFile(connector);

        FlinkEnvManager fem = new FlinkEnvManager(
                settings.getAbsolutePath(),
                "JOB:computeSentiments",
                "GeoTemporalIndex",
                "SentimentData"
        );

        DataSet<Tuple2<Key, Value>> rawTwitterDataRows = fem.getDataFromAccumulo();
        DataSet<Tuple3<String, Integer, String>> predictedSentiments = rawTwitterDataRows
                .flatMap(new StanfordSentimentFlatMap())
                ;

        // predictedSentiments --> sentiment table
        AccumuloOutputFormat accumuloOutputFormat = new AccumuloOutputFormat();
//        AccumuloOutputFormat.setConnectorInfo(
//                "JOB:computeSentiments",
//                "accumulo.user",
//                new PasswordToken("password")
//        );
//        accumuloOutputFormat

//        predictedSentiments.output();
//        predictedSentiments.writeAsCsv("file:///tmp/file", "\n", "|");
//        predictedSentiments.output(fem.getHadoopOF());
//        fem.getExecutionEnvironment().execute("JOB:computeSentiments");

//        BatchScanner scanner = connector.createBatchScanner("GeoTemporalIndex", authorizations, 20);
        Scanner scanner = connector.createScanner("SentimentData", authorizations);

        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.printf("Key : %-50s  Value : %s\n", entry.getKey(), entry.getValue());
        }
    }

    /**
     * Creates a settings file with data of Mini Accumulo Cluster.
     * @param connector Connector connects to an Accumulo instance to perform table operations.
     * @return A settings file for MiniAccumuloCluster
     * @throws IOException Gerneral exception thrown by input/output errors.
     */
    private static File createSettingsFile(Connector connector) throws IOException {
        tmpSettingsDir.create();
        File file = tmpSettingsDir.newFile("settings");
        FileOutputStream fos = new FileOutputStream(file, false);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fos));
        String parameters = "accumulo.instance=" + connector.getInstance().getInstanceName() + "\n"+
                "accumulo.user=" + connector.whoami() +"\n"+
                "accumulo.password=password\n"+
                "accumulo.zookeeper=" + connector.getInstance().getZooKeepers();
        br.write(parameters);
        br.flush();
        br.close();
        fos.flush();
        fos.close();
        return file;
    }

    /**
     * Returns a textual sentiment of a single tweet.
     * @param avgSentiment Average of each sentiment for each sentence within a tweet.
     * @return Textual sentiment of the tweet.
     */
    private static String getSentimentPolarity(int avgSentiment){
        String polarity = "";
        switch (avgSentiment) {
            case 0:
                polarity = "very negative";
                break;
            case 1:
                polarity = "negative";
                break;
            case 2:
                polarity = "neutral";
                break;
            case 3:
                polarity = "positive";
                break;
            case 4:
                polarity = "very positive";
                break;
            default:
                polarity = "error";
                break;
        }
        return polarity;

    }
}

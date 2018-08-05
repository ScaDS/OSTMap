package org.iidp.ostmap.analytics.sentiment_analysis;

import com.github.davidmoten.geo.GeoHash;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.spark.SparkConf;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.iidp.ostmap.analytics.sentiment_analysis.stanford.StopwordsLoader;
import org.iidp.ostmap.analytics.sentiment_analysis.util.MLlibSentimentAnalyzer;
import org.iidp.ostmap.analytics.sentiment_analysis.util.PropertiesLoaderJ;
import org.iidp.ostmap.analytics.sentiment_analysis.util.TwitterConnector;
import org.iidp.ostmap.commons.accumulo.AmcHelper;
import org.iidp.ostmap.commons.enums.AccumuloIdentifiers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.auth.OAuthAuthorization;
import twitter4j.conf.ConfigurationBuilder;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SparkBatchProcessingTest {

    private static PropertiesLoaderJ propertiesLoader = new PropertiesLoaderJ();

    private static StreamingContext ssc = SparkBatchProcessingTest.createSparkStreamingContext();

    // Directories for MiniAccumuloCluster
    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();
    private static TemporaryFolder tmpSettingsDir = new TemporaryFolder();

    // Helper class which provides a MiniAccumuloCluster as well as a Connector.
    private static AmcHelper amcHelper = new AmcHelper();

    public SparkBatchProcessingTest(){

    }

    @BeforeClass
    public static void setupCluster()
            throws TwitterException, AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        amcHelper.startMiniCluster(tmpDir.getRoot().getAbsolutePath());

        Connector connector = amcHelper.getConnector();
        System.out.printf("[ACCUMULO] I am \'%s\'.\n", connector.whoami());

        Authorizations authorizations = new Authorizations(AccumuloIdentifiers.AUTHORIZATION.toString());

        // retrieve tweets from ScaDS
        List<Status> statusList = TwitterConnector.getTweetsFromUser("Sca_DS");

        // create GeoTemporalIndex
        SparkBatchProcessingTest.createTableGeoTemporalIndex(connector, statusList);
    }

    @AfterClass
    public static void shutdownCluster(){
        amcHelper.stopMiniCluster();
    }

    @Test
    public void testQuery() throws IOException {
        System.out.println("I am here");


//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("EE MMM dd HH:mm:ss ZZ yyyy");
//
        NaiveBayesModel naiveBayesModel = NaiveBayesModel.load(ssc.sparkContext(), propertiesLoader.naiveBayesModelPath);
        List<String> stopWords = StopwordsLoader.loadStopwordsFromFile(propertiesLoader.stopwordsPath);


        OAuthAuthorization oAuth = SparkBatchProcessingTest.getOAuthoirzation(propertiesLoader);
//        List<Status> rawTweets = TwitterUtils.createStream(ssc, oAuth);
//
//        String delimiter = "|";
//        String tweetsClassifiedPath = propertiesLoader.tweetsClassifiedPath;
//        String classifiedTweets = rawTweets.filter(hasGeoLocation(status)).map(predictSentiments());
//
//        ssc.stop(true);
    }

    private static OAuthAuthorization getOAuthoirzation(PropertiesLoaderJ propertiesLoader){
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey(propertiesLoader.oAuthConsumerKey)
                .setOAuthConsumerSecret(propertiesLoader.oAuthConsumerSecret)
                .setOAuthAccessToken(propertiesLoader.oAuthAccessToken)
                .setOAuthAccessTokenSecret(propertiesLoader.oAuthAccessTokenSecret);
        return new OAuthAuthorization(configurationBuilder.build());
    }


    private static StreamingContext createSparkStreamingContext(){
        SparkConf conf = new SparkConf()
                .setAppName("Spark Batch Processing Test")
                .set("spark.serializer", KryoSerializer.class.getCanonicalName())
                .set("spark.eventLog.enabled", "true");
        return new StreamingContext(conf, Durations.seconds(15));
    }

    private static void predictSentiments(Status status, List<String> stopwords, NaiveBayesModel naiveBayesModel){
        String tweetText = status.getText().replaceAll("\n", "");
        Double sentiment = MLlibSentimentAnalyzer.computeSentiment(tweetText, stopwords, naiveBayesModel);


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
                tweetCoordinates = SparkBatchProcessingTest.getCoordinates(status);
                geohash = SparkBatchProcessingTest.getGeoHash(tweetCoordinates);
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

    private static Boolean hasGeoLocation(Status status){
        if (status.getGeoLocation() != null) {
            return true;
        }
        return false;
    }

}

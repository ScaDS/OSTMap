package org.iidp.ostmap.stream_processing;

import org.iidp.ostmap.stream_processing.functions.CalculateRawTwitterDataKey;
import org.iidp.ostmap.stream_processing.functions.DateExtraction;
import org.iidp.ostmap.stream_processing.functions.TermExtraction;
import org.iidp.ostmap.stream_processing.functions.UserExtraction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.iidp.ostmap.stream_processing.sinks.RawTwitterDataSink;
import org.iidp.ostmap.stream_processing.sinks.TermIndexSink;
import org.iidp.ostmap.stream_processing.types.RawTwitterDataKey;
import org.iidp.ostmap.stream_processing.types.SinkConfiguration;
import scala.Tuple2;

/**
 *         Entry point of the twitter stream persist.
 *         Connects to twitter stream and gets "all" tweets with geotag in the bounding box of germany.
 */
public class Driver {

    // names of the target accumulo tables
    private final String tableNameTerms = "TermIndex";
    private final String tableNameRawData = "RawTwitterData";
    // configuration values for accumulo
    private String accumuloInstanceName;
    private String accumuloZookeeper;
    // flag for usage of accumulo mini cluster
    private boolean runOnMAC = false;

    /**
     * empty constructor
     */
    public Driver() {
    }

    /**
     * defines the workflow and executes it
     *
     * @param pathToTwitterProperties
     * @param pathToAccumuloProperties
     * @throws Exception
     */
    public void run(String pathToTwitterProperties, String pathToAccumuloProperties) throws Exception {
        run(pathToTwitterProperties, pathToAccumuloProperties, null);
    }

    public void run(String pathToTwitterProperties, String pathToAccumuloProperties, String tweet) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // decide which stream source should be used
        DataStream<String> geoStream;
        if(tweet==null) {
            geoStream = env.addSource(new GeoTwitterSource(pathToTwitterProperties));
        }
        else {
            geoStream = env.fromElements ( tweet );
        }

        // decide which configuration should be used
        RawTwitterDataSink rtdSink = new RawTwitterDataSink();
        TermIndexSink tiSink = new TermIndexSink();
        SinkConfiguration sc;
        if(runOnMAC) {
            sc = SinkConfiguration.createConfigForMinicluster(accumuloInstanceName, accumuloZookeeper);
        }
        else {
            sc = SinkConfiguration.createConfigFromFile(pathToAccumuloProperties);
        }
        rtdSink.configure(sc, tableNameRawData);
        tiSink.configure(sc, tableNameTerms);

        // stream of tuples containing RawTwitterDataKey and tweet's json-String
        DataStream<Tuple2<RawTwitterDataKey, String>> rtdStream = geoStream
                                                                    .flatMap(new DateExtraction())
                                                                    .flatMap(new CalculateRawTwitterDataKey());

        // write into rawTwitterData-table
        rtdStream.addSink(rtdSink);

        // processing for user
        rtdStream
                .flatMap(new UserExtraction())
                .addSink(tiSink);
        // processing for terms
        rtdStream
                .flatMap(new TermExtraction())
                .addSink(tiSink);

        env.execute("twitter stream");
    }


    public void addMACdata(String accInstanceName, String accZookeeper)
    {
        runOnMAC = true;
        accumuloInstanceName = accInstanceName;
        accumuloZookeeper =  accZookeeper;
    }

    /**
     * starts the application
     *
     * @param args args[0] should be a string to a path containing a file with: twitter access data, args[1] is a path to a file with accumulo connection details
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("error: wrong arguments");
            System.out.println("need 2 paths: pathToTwitterProperties, pathToAccumuloProperties");
            return;
        }

        Driver driver = new Driver();
        driver.run(args[0], args[1]);
    }

}

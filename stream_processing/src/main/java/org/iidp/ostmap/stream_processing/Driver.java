package org.iidp.ostmap.stream_processing;


import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.iidp.ostmap.stream_processing.functions.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.iidp.ostmap.stream_processing.sinks.GeoTemporalIndexSink;
import org.iidp.ostmap.stream_processing.sinks.LanguageFrequencySink;
import org.iidp.ostmap.stream_processing.sinks.RawTwitterDataSink;
import org.iidp.ostmap.stream_processing.sinks.TermIndexSink;
import org.iidp.ostmap.stream_processing.types.RawTwitterDataKey;
import org.iidp.ostmap.stream_processing.types.SinkConfiguration;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;

/**
 *         Entry point of the twitter stream persist.
 *         Connects to twitter stream and gets "all" tweets with geotag in the bounding box of germany.
 */
public class Driver {

    // names of the target accumulo tables
    private final String tableNameTerms = "TermIndex";
    private final String tableNameRawData = "RawTwitterData";
    private final String tableNameFrequencies = "TweetFrequency";
    private final String tableNameGeoTemporal = "GeoTemporalIndex";
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

    public void run(String pathToTwitterProperties, String pathToAccumuloProperties, ArrayList<String> tweet) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // one watermark each ten second
        env.getConfig().setAutoWatermarkInterval(1000);

        // decide which stream source should be used
        DataStream<String> geoStream;
        if(tweet==null) {
            geoStream = env.addSource(new GeoTwitterSource(pathToTwitterProperties));
        }
        else {
            geoStream = env.fromCollection ( tweet );
        }

        // decide which configuration should be used
        RawTwitterDataSink rtdSink = new RawTwitterDataSink();
        TermIndexSink tiSink = new TermIndexSink();
        LanguageFrequencySink frqSink = new LanguageFrequencySink();
        GeoTemporalIndexSink gtiSink = new GeoTemporalIndexSink();
        SinkConfiguration sc;
        if(runOnMAC) {
            sc = SinkConfiguration.createConfigForMinicluster(accumuloInstanceName, accumuloZookeeper);
        }
        else {
            sc = SinkConfiguration.createConfigFromFile(pathToAccumuloProperties);
        }
        rtdSink.configure(sc, tableNameRawData);
        tiSink.configure(sc, tableNameTerms);
        frqSink.configure(sc, tableNameFrequencies);
        gtiSink.configure(sc, tableNameGeoTemporal);


        // stream of tuples containing timestamp and tweet's json-String
        DataStream<Tuple2<Long, String>> dateStream = geoStream.flatMap(new DateExtraction());

        dateStream
                .flatMap(new LanguageFrequencyRowExtraction())
                .flatMap(new LanguageTagExtraction())
                .assignTimestampsAndWatermarks(new TimestampExtractorForDateStream())
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply (new AllWindowFunctionLangFreq())
                .addSink(frqSink);

        // stream of tuples containing RawTwitterDataKey and tweet's json-String
        DataStream<Tuple2<RawTwitterDataKey, String>> rtdStream = dateStream.flatMap(new CalculateRawTwitterDataKey());

        /** write into rawTwitterData-table */
        rtdStream.addSink(rtdSink);

        /** write into geoTemporalIndex-table */
        rtdStream
                .flatMap(new GeoTemporalKeyExtraction())
                .addSink(gtiSink);

        /** write into termIndex-table */
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

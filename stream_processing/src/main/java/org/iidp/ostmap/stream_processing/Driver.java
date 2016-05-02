package org.iidp.ostmap.stream_processing;

import org.iidp.ostmap.stream_processing.functions.KeyExtraction;
import org.iidp.ostmap.stream_processing.functions.DateExtraction;
import org.iidp.ostmap.stream_processing.functions.UserExtraction;
import org.iidp.ostmap.stream_processing.sinks.AccumuloSink;
import org.apache.commons.io.IOUtils;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * @author Martin Grimmer (martin.grimmer@mgm-tp.com)
 *         <p>
 *         Entry point of the twitter stream persist.
 *         Connects to twitter stream and gets "all" tweets with geotag in the bounding box of germany.
 */
public class Driver {

    // name of the target accumulo table
    //public static final String tableName = "RawTwitterData";
    public static final String tableName = "TestData";
    public static  String accumuloInstanceName;
    public static  String accumuloZookeeper;

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

        DataStream<String> geoStream;
        if(tweet==null) {
            geoStream = env.addSource(new GeoTwitterSource(pathToTwitterProperties));
        }
        else
        {
            geoStream = env.fromElements ( tweet );
        }

        AccumuloSink sink = new AccumuloSink();
        sink.configure(pathToAccumuloProperties, tableName);

        sink.configure(tableName, accumuloInstanceName, accumuloZookeeper);
        geoStream
                .flatMap(new DateExtraction())
                .flatMap(new UserExtraction())
                .flatMap(new KeyExtraction())
                .addSink(sink);


        env.execute("twitter stream");
    }


    public void addMACdata(String accInstanceName, String accumuloZK)
    {
        accumuloInstanceName = accInstanceName;
        accumuloZookeeper =  accumuloZK;
    }

    /**
     * starts the application
     *
     * @param args args[0] should be a string to a path containing a file with: twitter access data, args[1] is a path to a file with accumulo connection details
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("error: wrong arguments");
            System.out.println("need 3 paths: pathToTwitterProperties1, pathToAccumuloProperties");
            return;
        }

        Driver driver = new Driver();
        driver.run(args[0], args[1]);
    }

}

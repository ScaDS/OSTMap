package com.mgm.ring;

import com.mgm.ring.functions.CalculateKey;
import com.mgm.ring.functions.DateExtraction;
import com.mgm.ring.sinks.AccumuloSink;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Martin Grimmer (martin.grimmer@mgm-tp.com)
 *         <p>
 *         Entry point of the twitter stream persist.
 *         Connects to twitter stream and gets "all" tweets with geotag in the bounding box of germany.
 */
public class Driver {

    // name of the target accumulo table
    public static final String tableName = "RawTwitterData";

    /**
     * empty constructor
     */
    public Driver() {
    }

    /**
     * defines the workflow and executes it
     *
     * @param pathToTwitterProperties1
     * @param pathToAccumuloProperties
     * @throws Exception
     */
    public void run(String pathToTwitterProperties1, String pathToAccumuloProperties) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> geoStream = env.addSource(new GeoTwitterSource(pathToTwitterProperties1));

        AccumuloSink sink = new AccumuloSink();
        sink.configure(pathToAccumuloProperties, tableName);

        geoStream
                .flatMap(new DateExtraction())
                .map(new CalculateKey())
                .addSink(sink);

        env.execute("twitter stream");
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

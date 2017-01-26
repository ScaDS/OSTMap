package org.iidp.ostmap.stream_processing;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * simple app to test the connection to the twitter stream
 */
public class TwitterConnectionTest {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> geoStream;
        geoStream = env.addSource(new GeoTwitterSource(args[0]));
        geoStream.print();
        env.execute("twitter stream connection test");
    }

}

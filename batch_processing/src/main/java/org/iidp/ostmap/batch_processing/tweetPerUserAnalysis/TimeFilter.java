package org.iidp.ostmap.batch_processing.tweetPerUserAnalysis;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.nio.ByteBuffer;


/**
 * filters RawTwitterData entrys to select a timeframe
 */
public class TimeFilter implements FilterFunction<Tuple2<Key,Value>>, Serializable {

    private long timeMin;
    private long timeMax;

    public TimeFilter(long timeMin, long timeMax){
        this.timeMin = timeMin;
        this.timeMax = timeMax;
    }


    @Override
    public boolean filter(Tuple2<Key, Value> entry) throws Exception {


        //the key should begin with a long timestamp
        ByteBuffer bb = ByteBuffer.wrap(entry.f0.getRowData().toArray());
        long timeStamp = bb.getLong();

        return timeMin < timeStamp && timeStamp < timeMax;
    }
}

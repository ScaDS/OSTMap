package org.iidp.ostmap.stream_processing.functions;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import scala.Tuple3;

/**
 * timestamps and watermarks needed for the windowed stream
 * careful: timestamps in flink are in milliseconds
 */
public class TimestampExtractorForDateStream implements AssignerWithPeriodicWatermarks<Tuple3<Long, String, String>> {

    long maxOutOfOrderness = 10000; // seconds
    long currentMaxTimestamp = Long.MIN_VALUE; // timestamp in seconds since 1970


    @Override
    public long extractTimestamp(Tuple3<Long, String, String> element, long previousElementTimestamp) {
        long currentTimestamp = element._1() * 1000l;
        if (currentTimestamp > currentMaxTimestamp) {
            currentMaxTimestamp = currentTimestamp;
        }
        return currentTimestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

}
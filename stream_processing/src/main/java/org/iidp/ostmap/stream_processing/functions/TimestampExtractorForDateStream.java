package org.iidp.ostmap.stream_processing.functions;

import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import scala.Tuple3;

/**
 * AscendingTimestampExtractor as preparation for the windowed stream
 */
public class TimestampExtractorForDateStream extends AscendingTimestampExtractor<Tuple3<Long, String, String>> {


    @Override
    public long extractAscendingTimestamp(Tuple3<Long, String, String> element) {
        Long ts = element._1(); // Long.parseLong(element._3());
        return ts;
        }


}
package org.iidp.ostmap.stream_processing.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.iidp.ostmap.commons.accumulo.geoTemp.GeoTemporalKey;
import org.iidp.ostmap.stream_processing.types.RawTwitterDataKey;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Extracts GeoTemporalKey
 */
public class GeoTemporalKeyExtraction implements FlatMapFunction<Tuple2<RawTwitterDataKey, String>, Tuple2<RawTwitterDataKey, GeoTemporalKey>>, Serializable {

    /**
     * empty constructor for serialization (needed by flink)
     */
    public GeoTemporalKeyExtraction() {
    }

    /**
     * Extract GeoTemporalKey for each tweet
     * @param input     tuple of rawTwitterDataKey and tweet-json
     * @param out       tuple of rawTwitterDataKey and GeoTemporalKey
     * @throws Exception
     */
    @Override
    public void flatMap(Tuple2<RawTwitterDataKey, String> input, Collector<Tuple2<RawTwitterDataKey, GeoTemporalKey>> out) throws Exception {

        // extract geoTempKey
        GeoTemporalKey geoTempKey = GeoTemporalKey.buildKey(input._2());

        if (geoTempKey.rowBytes!=null && geoTempKey.columQualifier!=null) {
            out.collect(new Tuple2(input._1(), geoTempKey));
        }

    }

}

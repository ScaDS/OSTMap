package org.iidp.ostmap.batch_processing.geoTempConverter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.Text;
import org.iidp.ostmap.commons.accumulo.geoTemp.GeoTemporalKey;

/**
 * maps RawTwitterData rows to GeoTimeIndex mutations
 */
public class GeoTempFlatMap implements FlatMapFunction<Tuple2<Key, Value>, Tuple2<Text, Mutation>> {
    private String outputTableName;
    private static final byte[] EMPTY_BYTES = new byte[0];

    public GeoTempFlatMap(String outputTableName) {
        this.outputTableName = outputTableName;
    }

    @Override
    public void flatMap(Tuple2<Key, Value> value, Collector<Tuple2<Text, Mutation>> out) throws Exception {

        GeoTemporalKey gtk = GeoTemporalKey.buildKey(value.f1.toString());

        if (gtk.rowBytes != null && gtk.columQualifier != null) {
            //create mutations for username and screen name
            Mutation m = new Mutation(gtk.rowBytes);
            m.put(value.f0.getRow().getBytes(), gtk.columQualifier, EMPTY_BYTES);
            out.collect(new Tuple2<>(new Text(outputTableName), m));
        }
    }
}

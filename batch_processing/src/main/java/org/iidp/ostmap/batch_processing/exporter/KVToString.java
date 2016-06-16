package org.iidp.ostmap.batch_processing.exporter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;


/**
 * maps RawTwitterData rows to Strings containing the Json String
 */
public class KVToString implements MapFunction<Tuple2<Key, Value>, String>, Serializable {
    @Override
    public String map(Tuple2<Key, Value> value) throws Exception {
        return value.f1.toString();
    }
}

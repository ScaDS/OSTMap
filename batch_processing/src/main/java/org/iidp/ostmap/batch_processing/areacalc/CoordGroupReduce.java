package org.iidp.ostmap.batch_processing.areacalc;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.Serializable;


public class CoordGroupReduce implements GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>>, Serializable {

    @Override
    public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, /*TODO POJO*/String>> out) throws Exception {

    }

}

package org.iidp.ostmap.batch_processing.areacalc;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.TreeSet;


public class CoordGroupReduce implements GroupReduceFunction<Tuple2<String, String>, Tuple2<String, String>>, Serializable {
    private String user = "";
    private String coords = "";
    private TreeSet<String> coordSet = new TreeSet<>();
    @Override
    public void reduce(Iterable<Tuple2<String, String>> values, Collector<Tuple2<String, /*TODO POJO*/String>> out) throws Exception {
        for (Tuple2<String,String> entry: values) {
            if (coords.equals("")) {
                coords += entry.f1;
            } else {
                coords += "|" + entry.f1;
            }
            user = entry.f0;
            coordSet.add(entry.f1.toString());
        }
        if(coordSet.size() > 2){
            out.collect(new Tuple2<String,String>(user,coords));
        }

    }

}

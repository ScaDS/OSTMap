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
        coords = "";
        for (Tuple2<String,String> entry: values) {
            user = entry.f0;
            coordSet.add(entry.f1.toString());
        }
        if(coordSet.size() > 2){
            coords = coordSet.first();
            coordSet.remove(coordSet.first());
            while(coordSet.size() > 0){
                coords += "|" + coordSet.first();
                coordSet.remove(coordSet.first());
            }
            out.collect(new Tuple2<String,String>(user,coords));
        }

    }

}

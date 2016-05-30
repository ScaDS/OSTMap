package org.iidp.ostmap.batch_processing.pathcalc;

import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;

import java.io.Serializable;

public class TopTenGroupReduce implements GroupReduceFunction<Tuple3<String, Double, Integer>, Tuple2<Text, Mutation>>, Serializable{

    int i = 0;
    String toReturn = "";
    String rowkey = "";
    public TopTenGroupReduce(String rowkey){
        this.rowkey = rowkey;

    }
        @Override
        public void reduce(Iterable<Tuple3<String, Double, Integer>> values, Collector<Tuple2<Text, Mutation>> out) throws Exception {
            for (Tuple3<String,Double,Integer> entry: values) {
                if(i == 0){
                    toReturn = "[" + entry.f0.toString();
                }else{
                    toReturn = toReturn + "," + entry.f0.toString();
                }
                i++;
                if(i == 10){
                    break;
                }

            }
            toReturn += "]";

            Mutation m = new Mutation(rowkey);
            m.put(rowkey, "", new Value(toReturn.getBytes()));
            out.collect(new Tuple2<>(new Text("HighScore"), m));


        }
}

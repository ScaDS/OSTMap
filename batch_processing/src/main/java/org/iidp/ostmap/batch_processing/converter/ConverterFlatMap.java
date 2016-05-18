package org.iidp.ostmap.batch_processing.converter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.Text;
import org.codehaus.jettison.json.JSONException;
import org.iidp.ostmap.commons.tokenizer.Tokenizer;
import org.codehaus.jettison.json.JSONObject;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


/**
 * maps RawTwitterData rows to mutations to insert into TermIndex
 */
public class ConverterFlatMap implements FlatMapFunction<Tuple2<Key, Value>, Tuple2<Text,Mutation>>, Serializable {

    private Tokenizer tokenizer;
    private String outputTableName;

    public ConverterFlatMap(Tokenizer tokenizer, String outputTableName){
        this.tokenizer = tokenizer;
        this.outputTableName = outputTableName;
    }

    @Override
    public void flatMap(Tuple2<Key, Value> in, Collector<Tuple2<Text, Mutation>> out) {

        JSONObject obj = null;
        String userName = "";
        String userScreenName = "";
        String text = "";

        try {
            obj = new JSONObject(in.f1.toString());
            text = obj.getString("text");
            userScreenName = obj.getJSONObject("user").getString("screen_name");
            userName = obj.getJSONObject("user").getString("name");

        } catch (JSONException e) {
            e.printStackTrace();
        }


        Map<String, Integer> tokenCount= new HashMap<>();

        //count token occurences
        for(String token: tokenizer.tokenizeString(text)){

            if(tokenCount.containsKey(token)){

                tokenCount.put(token,tokenCount.get(token)+1);

            }else{
                tokenCount.put(token,1);
            }
        }

        //create mutations for tokens
        for(Map.Entry<String, Integer> kv: tokenCount.entrySet()){
            Mutation m = new Mutation(kv.getKey());
            if(kv.getValue() > 1){

                m.put("text", in.f0.getRow().toString(), kv.getValue().toString());
            }else{
                m.put("text", in.f0.getRow().toString(), new Value());
            }
            out.collect(new Tuple2<>(new Text(outputTableName), m));
        }


        //create mutations for username and screen name
        Mutation m = new Mutation(userName.toLowerCase());
        m.put("user", in.f0.getRow().toString(), new Value());
        out.collect(new Tuple2<>(new Text(outputTableName), m));

        m = new Mutation(userScreenName.toLowerCase());
        m.put("user", in.f0.getRow().toString(), new Value());
        out.collect(new Tuple2<>(new Text(outputTableName), m));

    }
}

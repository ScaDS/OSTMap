package org.iidp.ostmap.batch_processing;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.Text;
import org.codehaus.jettison.json.JSONException;
import org.iidp.ostmap.commons.Tokenizer;
import org.codehaus.jettison.json.JSONObject;


/**
 * Converts RawTwitterData rows to TermIndex rows
 */
public class Converter implements FlatMapFunction<Tuple2<Key, Value>, Tuple2<Key, Value>> {

    private Tokenizer tokenizer;

    public Converter(Tokenizer tokenizer){
        this.tokenizer = tokenizer;
    }

    @Override
    public void flatMap(Tuple2<Key, Value> in, Collector<Tuple2<Key, Value>> out) {

        JSONObject obj = null;
        String user = "";
        String text = "";

        try {
            obj = new JSONObject(in.f1.toString());
            text = obj.getString("text");
            user = obj.getString("user");
        } catch (JSONException e) {
            e.printStackTrace();
        }


        //create output rows from tokenized tweet text
        for(String token: tokenizer.tokenizeString(text)){

            Key key = new Key(new Text(token), new Text("text"), in.f0.getRow());

            out.collect(new Tuple2<Key, Value>(key, new Value()));
        }

        //create output row for user
        Key key = new Key(new Text(user), new Text("user"), in.f0.getRow());

        out.collect(new Tuple2<Key, Value>(key, new Value()));

    }
}

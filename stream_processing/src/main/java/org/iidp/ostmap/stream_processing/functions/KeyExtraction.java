package org.iidp.ostmap.stream_processing.functions;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.iidp.ostmap.stream_processing.types.CustomKey;
import org.apache.commons.codec.Charsets;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;

/**
 * class calculating the custom key (for accumulo) for a given tweet with timestamp
 *
 * @author Martin Grimmer (martin.grimmer@mgm-tp.com)
 *
 *
 */
public class KeyExtraction implements FlatMapFunction<Tuple3<Long, String, String>, Tuple2<CustomKey, Integer>>, Serializable {

    // hash function to use (murmurhash) see https://github.com/google/guava/wiki/HashingExplained
    private static HashFunction hash = Hashing.murmur3_32();

    @Override
    public void flatMap(Tuple3<Long, String, String> inTuple, Collector<Tuple2<CustomKey, Integer>> out) throws Exception {
        int hash = getHash(inTuple._2());
        CustomKey key = CustomKey.buildCustomKey(inTuple._1(), hash, inTuple._2());

        //Collect user-entry
        key.type = CustomKey.TYPE_USER;
        key.row = inTuple._3();
        out.collect(new Tuple2<>(key, 0));

        //Collect all text-entries
        String text = getText(inTuple._2());
        key.type = CustomKey.TYPE_TEXT;
        org.iidp.ostmap.stream_processing.functions.Tokenizer tokenizer = new org.iidp.ostmap.stream_processing.functions.Tokenizer();
        ArrayList<String> list = (ArrayList<String>) tokenizer.tokenizeString(text);
        for(String s : tokenizer.tokenizeString(text))
            {
                key.row = s;
                int count = Collections.frequency(list, s);
                out.collect(new Tuple2<>(key, count));
            }
    }

    /**
     * return a int representing the hashcode of the given string
     *
     * @param json to calculate the hash for
     * @return the int hash
     */
    public int getHash(String json) {
        int bufferSize = json.length();
        ByteBuffer bb = ByteBuffer.allocate(bufferSize);
        bb.put(json.getBytes(Charsets.UTF_8));
        return hash.hashBytes(bb.array()).asInt();
    }

    public String getText(String tweetJson)
    {
        int pos1 = tweetJson.indexOf("\"text\":\"");
        int pos2 = pos1 + 8;
        int pos3 = tweetJson.indexOf("\",\"", pos2);
        return tweetJson.substring(pos2, pos3);
    }


}

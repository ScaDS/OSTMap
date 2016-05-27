package org.iidp.ostmap.stream_processing.functions;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.commons.codec.Charsets;
import org.iidp.ostmap.commons.accumulo.keys.RawTwitterDataKey;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * class calculating the custom key (for accumulo) for a given tweet with timestamp
 *
 */
public class CalculateRawTwitterDataKey implements FlatMapFunction<Tuple2<Long, String>, Tuple2<RawTwitterDataKey, String>>, Serializable {

    // hash function to use (murmurhash) see https://github.com/google/guava/wiki/HashingExplained
    private static HashFunction hash = Hashing.murmur3_32();

    /**
     * Extracting key and collecting for all tokens
     * @param inTuple   tuple of timestamp and tweet's json-string
     * @param out       tuple of RawTwitterKey and tweet-string
     * @throws Exception
     */
    @Override
    public void flatMap(Tuple2<Long, String> inTuple, Collector<Tuple2<RawTwitterDataKey, String>> out) throws Exception {
        int hash = getHash(inTuple._2());
        RawTwitterDataKey rtdKey = RawTwitterDataKey.buildRawTwitterDataKey(inTuple._1(), hash);

        //Collect user-entry
        out.collect(new Tuple2<>(rtdKey, inTuple._2()));
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


}

package com.mgm.ring.functions;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.mgm.ring.types.CustomKey;
import org.apache.commons.codec.Charsets;
import org.apache.flink.api.common.functions.MapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * class calculating the custom key (for accumulo) for a given tweet with timestamp
 *
 * @author Martin Grimmer (martin.grimmer@mgm-tp.com)
 *
 *
 */
public class CalculateKey implements MapFunction<Tuple2<Long, String>, Tuple2<CustomKey, String>>, Serializable {

    // hash function to use (murmurhash) see https://github.com/google/guava/wiki/HashingExplained
    private static HashFunction hash = Hashing.murmur3_32();

    @Override
    public Tuple2<CustomKey, String> map(Tuple2<Long, String> inTuple) throws Exception {
        int hash = getHash(inTuple._2());
        CustomKey key = CustomKey.buildCustomKey(inTuple._1(), hash);
        return new Tuple2<>(key, inTuple._2());
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

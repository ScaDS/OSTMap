package org.iidp.ostmap.commons.accumulo.keys;

import java.nio.ByteBuffer;

/**
 * Contains data for one key of the rawTwitterData-table
 */
public class RawTwitterDataKey {

    // the tweet's timpestamp
    public long timestamp;

    // hash of the tweet's json (murmurhash)
    public int hash;

    // bytes of timestamp and hash
    public byte[] keyBytes;

    // empty constructor
    public RawTwitterDataKey() {}

    /**
     *
     * @param timestamp seconds since 1970
     * @return RawTwitterDataKey row prefix for given timestamp
     */
    public static byte[] buildPrefix(String timestamp){
        return buildPrefix(Long.parseLong(timestamp));
    }

    /**
     *
     * @param timestamp seconds since 1970
     * @return RawTwitterDataKey row prefix for given timestamp
     */
    public static byte[] buildPrefix(Long timestamp){
        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
        bb.putLong(timestamp);

        return bb.array();
    }

    /**
     * builds a rawTwitterDataKey for the given parameter
     * @param timestamp of the tweet
     * @param hash      int hash of the tweet
     * @return a new key object for rawTwitterData-table
     */
    public static RawTwitterDataKey buildRawTwitterDataKey(long timestamp, int hash) {
        RawTwitterDataKey key = new RawTwitterDataKey();
        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        bb.putLong(timestamp).putInt(hash);
        key.timestamp = timestamp;
        key.hash = hash;
        key.keyBytes = bb.array();
        return key;
    }

}

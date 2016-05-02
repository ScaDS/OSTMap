package org.iidp.ostmap.stream_processing.types;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * key representing a tweet in accumulo
 * consists of: 8 byte timestamp of the tweet and 4 byte hash of the tweet
 *
 * @author Martin Grimmer (martin.grimmer@mgm-tp.com)
 */
public class CustomKey implements Serializable {

    public static final String TYPE_TEXT = "text";
    public static final String TYPE_USER = "user";

    // the foreign-key as byte array
    public byte[] foreignKeyBytes;

    // the row
    public String row;

    // type of the token (text/user)
    public String type;

    /**
     * empty constructor for flink
     */
    public CustomKey() {
    }

    /**
     * builds a custom key for the given parameter
     *
     * @param timestamp of the tweet
     * @param hash      int hash of the tweet
     * @return a new custom key object
     */
    public static CustomKey buildCustomKey(long timestamp, int hash, String tweet) {
        CustomKey key = new CustomKey();
        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        bb.putLong(timestamp).putInt(hash);
        key.foreignKeyBytes = bb.array();
        return key;
    }

    @Override
    /**
     * returns a human readable string representing the custom key
     */
    public String toString() {
        return "CustomKey{" +
                "foreignKeyBytes=" + Arrays.toString(foreignKeyBytes) +
                '}';
    }


}

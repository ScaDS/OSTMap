package org.iidp.ostmap.commons.accumulo;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.codec.Charsets;
import org.apache.flink.util.Collector;
import org.iidp.ostmap.stream_processing.functions.CalculateRawTwitterDataKey;
import org.iidp.ostmap.commons.accumulo.keys.RawTwitterDataKey;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.Arrays;

import static org.iidp.ostmap.stream_processing.functions.DateExtraction.formatterExtract;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Testclass for CalculateRawTwitterDataKey
 * Tests extraction of key-components
 */


public class KeyExtractionTest {


    // hash function to use (murmurhash) see https://github.com/google/guava/wiki/HashingExplained
    private static HashFunction hashFunction = Hashing.murmur3_32();
    private static long ts;
    private static String tweet;
    private static int hash;
    private static byte[] bytes;

    @BeforeClass
    public static void calcData() throws AccumuloException, AccumuloSecurityException, InterruptedException, IOException {

        ZonedDateTime time = ZonedDateTime.parse("Fri Apr 29 09:05:55 +0000 2016", formatterExtract);
        ts = time.toEpochSecond();
        tweet = "{\"created_at\":\"Fri Apr 29 09:05:55 +0000 2016\",\"id\":725974381906804738,\"id_str\":\"725974381906804738\",\"text\":\"Das sage ich dir gleich, das funktioniert doch nie! #haselnuss\",\"user\":{\"id\":179905182,\"name\":\"Peter Tosh\",\"screen_name\":\"PeTo\"}}";

        int bufferSize = tweet.length();
        ByteBuffer bb1 = ByteBuffer.allocate(bufferSize);
        bb1.put(tweet.getBytes(Charsets.UTF_8));
        hash = hashFunction.hashBytes(bb1.array()).asInt();

        ByteBuffer bb2 = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        bb2.putLong(ts).putInt(hash);
        bytes = bb2.array();
    }

    @Test
    public void testSomething() throws Exception {

        CalculateRawTwitterDataKey cRtd = new CalculateRawTwitterDataKey();
        Tuple2<Long, String> inTuple = new Tuple2<>(ts, tweet);


        Collector collector = new Collector<Tuple2<RawTwitterDataKey, Integer>>() {
            @Override
            public void collect(Tuple2<RawTwitterDataKey, Integer> record) {
                assertEquals(record._2, tweet);
                assertEquals(record._1.hash, hash);
                assertEquals(record._1.timestamp, ts);
                assertTrue(Arrays.equals(record._1.keyBytes, bytes));
            }

            @Override
            public void close() {

            }
        };
        cRtd.flatMap(inTuple, collector);


    }
}

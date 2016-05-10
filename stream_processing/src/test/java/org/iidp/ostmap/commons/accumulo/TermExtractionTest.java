package org.iidp.ostmap.commons.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.flink.util.Collector;
import org.iidp.ostmap.stream_processing.functions.TermExtraction;
import org.iidp.ostmap.commons.tokenizer.*;
import org.iidp.ostmap.stream_processing.types.RawTwitterDataKey;
import org.iidp.ostmap.stream_processing.types.TermIndexKey;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Testclass for TermExtraction
 * Tests extraction of all tokens
 */


public class TermExtractionTest {

    private static String tweet;
    private static ArrayList<String> listOfTokens;

    @BeforeClass
    public static void prepareData() throws AccumuloException, AccumuloSecurityException, InterruptedException, IOException {
        tweet = "{\"created_at\":\"Fri Apr 29 09:05:55 +0000 2016\",\"id\":725974381906804738,\"id_str\":\"725974381906804738\",\"text\":\"Das sage ich dir gleich, das funktioniert doch nie! #haselnuss\",\"user\":{\"id\":179905182,\"name\":\"Peter Tosh\",\"screen_name\":\"PeTo\",}}";
        String tweet_text = "Das sage ich dir gleich, das funktioniert doch nie! #haselnuss";
        Tokenizer t = new Tokenizer();
        listOfTokens = (ArrayList<String>) t.tokenizeString(tweet_text);
    }

    @Test
    public void testSomething() throws Exception {


        TermExtraction tE = new TermExtraction();
        RawTwitterDataKey rtdKey = new RawTwitterDataKey();
        Tuple2<RawTwitterDataKey, String> input = new Tuple2<>(rtdKey, tweet);


        Collector collector = new Collector<Tuple2<TermIndexKey, Integer>>() {
            @Override
            public void collect(Tuple2<TermIndexKey, Integer> record) {
                assertTrue(listOfTokens.contains(record._1.term));
                assertEquals(record._1.source, TermIndexKey.SOURCE_TYPE_TEXT);
                listOfTokens.remove(record._1.term);
            }

            @Override
            public void close() {
                assertTrue(listOfTokens.size()==0);
            }
        };

        tE.flatMap(input, collector);


    }
}

package org.iidp.ostmap.commons.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.flink.util.Collector;
import org.iidp.ostmap.stream_processing.functions.KeyExtraction;
import org.iidp.ostmap.stream_processing.functions.Tokenizer;
import org.iidp.ostmap.stream_processing.types.CustomKey;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;

import static org.iidp.ostmap.stream_processing.functions.DateExtraction.formatter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Testclass for KeyExtraction
 * Tests extraction of key-components and count
 */


public class KeyExtractionTest {

    private static ZonedDateTime time;
    private static long key;
    private static String tweet;
    private static ArrayList<String> list;

    @BeforeClass
    public static void calcData() throws AccumuloException, AccumuloSecurityException, InterruptedException, IOException {
        time = ZonedDateTime.parse("Fri Apr 29 09:05:55 +0000 2016", formatter);
        key = time.toEpochSecond();
        tweet = "{\"created_at\":\"Fri Apr 29 09:05:55 +0000 2016\",\"id\":725974381906804738,\"id_str\":\"725974381906804738\",\"text\":\"Das sage ich dir gleich, das funktioniert doch nie! #haselnuss\",\"user\":{\"id\":179905182,\"name\":\"Peter Tosh\",\"screen_name\":\"PeTo\"}}";
        Tokenizer t = new Tokenizer();
        String text = "Das sage ich dir gleich, das funktioniert doch nie! #haselnuss";
        list = (ArrayList) t.tokenizeString(text);
    }

    @Test
    public void testSomething() throws Exception {

        KeyExtraction dE = new KeyExtraction();
        Tuple3<Long, String, String> inTuple = new Tuple3<>(11L, tweet, "Peter Tosh");


        Collector collector = new Collector<Tuple2<CustomKey, Integer>>() {
            @Override
            public void collect(Tuple2<CustomKey, Integer> record) {
                //record is in list of tokens
                if(record._1.type==CustomKey.TYPE_TEXT) {
                    assertTrue(list.contains(record._1.row));
                }

                //user is Peter Tosh
                else {
                    assertEquals("Peter Tosh", record._1.row);
                }

                //Count for 'das' is 2
                if(record._1.row.equals("das") && record._1.type==CustomKey.TYPE_TEXT){
                    assertTrue(2==record._2);
                }
            }

            @Override
            public void close() {

            }
        };
        dE.flatMap(inTuple, collector);


    }
}

package org.iidp.ostmap.commons.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.flink.util.Collector;
import org.iidp.ostmap.stream_processing.functions.UserExtraction;
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
 * Testclass for UserExtraction
 * Tests extraction of both user names
 **/


public class UserExtractionTest {

    private static String tweet;
    private static ArrayList<String> listOfUsernames = new ArrayList<>();

    @BeforeClass
    public static void prepareData() throws AccumuloException, AccumuloSecurityException, InterruptedException, IOException {
        tweet = "{\"created_at\":\"Fri Apr 29 09:05:55 +0000 2016\",\"id\":725974381906804738,\"id_str\":\"725974381906804738\",\"text\":\"Das sage ich dir gleich, das funktioniert doch nie! #haselnuss\",\"user\":{\"id\":179905182,\"name\":\"Peter Tosh\",\"screen_name\":\"PeTo\"}}";
        listOfUsernames.add("peter tosh");
        listOfUsernames.add("peto");
    }

    @Test
    public void testSomething() throws Exception {


        UserExtraction uE = new UserExtraction();
        RawTwitterDataKey rtdKey = new RawTwitterDataKey();
        Tuple2<RawTwitterDataKey, String> input = new Tuple2<>(rtdKey, tweet);


        Collector collector = new Collector<Tuple2<TermIndexKey, Integer>>() {
            @Override
            public void collect(Tuple2<TermIndexKey, Integer> record) {
                assertTrue(listOfUsernames.contains(record._1.term));
                assertEquals(record._1.source, TermIndexKey.SOURCE_TYPE_USER);
                listOfUsernames.remove(record._1.term);
            }

            @Override
            public void close() {
                assertTrue(listOfUsernames.size()==0);
            }
        };

        uE.flatMap(input, collector);


    }
}

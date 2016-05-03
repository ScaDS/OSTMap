package org.iidp.ostmap.commons.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.flink.util.Collector;
import org.iidp.ostmap.stream_processing.functions.UserExtraction;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Testclass for DateExtraction
 * Tests extraction of date from json-string
 */


public class UserExtractionTest {

    private static String tweet;

    @BeforeClass
    public static void calcData() throws AccumuloException, AccumuloSecurityException, InterruptedException, IOException {
        tweet = "{\"created_at\":\"Fri Apr 29 09:05:55 +0000 2016\",\"id\":725974381906804738,\"id_str\":\"725974381906804738\",\"text\":\"Das sage ich dir gleich, das funktioniert doch nie! #haselnuss\",\"user\":{\"id\":179905182,\"name\":\"Peter Tosh\",\"screen_name\":\"PeTo\"}}";

    }

    @Test
    public void testSomething() throws Exception {

        UserExtraction uE = new UserExtraction();
        Tuple2<Long, String> input = new Tuple2<>(11L, tweet);


        Collector collector = new Collector<Tuple3<Long, String, String>>() {
            @Override
            public void collect(Tuple3<Long, String, String> record) {
                assertTrue(record._1()==input._1);
                assertEquals(input._2, record._2());
                assertEquals("peter tosh", record._3());
            }

            @Override
            public void close() {

            }
        };

        uE.flatMap(input, collector);


    }
}

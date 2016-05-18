package org.iidp.ostmap.commons.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.flink.util.Collector;
import org.iidp.ostmap.stream_processing.functions.DateExtraction;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.IOException;
import java.time.ZonedDateTime;

import static org.iidp.ostmap.stream_processing.functions.DateExtraction.formatterExtract;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Testclass for DateExtraction
 * Tests extraction of date from json-string
 */


public class DateExtractionTest {

    private static ZonedDateTime time;
    private static long key;
    private static long keyF;
    private static String tweet;

    @BeforeClass
    public static void calcData() throws AccumuloException, AccumuloSecurityException, InterruptedException, IOException {
        time = ZonedDateTime.parse("Fri Apr 29 09:05:55 +0000 2016", formatterExtract);
        key = time.toEpochSecond();
        keyF = 1461920755255L;
        tweet = "{\"created_at\":\"Fri Apr 29 09:05:55 +0000 2016\",\"id\":725974381906804738,\"id_str\":\"725974381906804738\",\"text\":\"Das sage ich dir gleich, das funktioniert doch nie! #haselnuss\",\"user\":{\"id\":179905182,\"name\":\"Peter Tosh\",\"screen_name\":\"PeTo\"}}";

    }

    @Test
    public void testSomething() throws Exception {

        DateExtraction dE = new DateExtraction();
        Collector collector = new Collector<Tuple2<Long, String>>() {
            @Override
            public void collect(Tuple2<Long, String> record) {
                assertTrue(key == record._1);
                assertEquals(tweet, record._2);
            }

            @Override
            public void close() {

            }
        };
        dE.flatMap(tweet, collector);


    }
}

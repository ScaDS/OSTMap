package org.iidp.ostmap.commons.accumulo.geoTemp;

import com.github.davidmoten.geo.GeoHash;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GeoTemporalTweetQueryTest {

    @Test
    public void mergeHashesTest(){
        Set<String> testList = new HashSet<>();

        testList.add("th04b501");
        testList.add("th04b503");
        testList.add("th04b502");
        testList.add("th06vehc");
        testList.add("th04b500");
        testList.add("th04b505");

        GeoTemporalTweetQuery gtq = new GeoTemporalTweetQuery();
        List<Tuple2<String,String>> sorted = gtq.mergeHashes(testList);

        assertEquals(3, sorted.size());

        assertEquals("th04b500", sorted.get(0).f0);
        assertEquals("th04b503", sorted.get(0).f1);
    }


    @Test
    public void getNextHashTest(){

        //System.out.println(GeoTemporalTweetQuery.getNextHash(GeoHash.encodeHash(23.0,45.6,8)));

        assertEquals("th04b501",GeoTemporalTweetQuery.getNextHash(GeoHash.encodeHash(23.0,45.0,8)));
        assertEquals("th06vehc",GeoTemporalTweetQuery.getNextHash(GeoHash.encodeHash(23.0,45.6,8)));

    }
}

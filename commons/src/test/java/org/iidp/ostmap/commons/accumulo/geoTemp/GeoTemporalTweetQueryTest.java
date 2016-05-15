package org.iidp.ostmap.commons.accumulo.geoTemp;

import com.github.davidmoten.geo.GeoHash;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GeoTemporalTweetQueryTest {

    @Test
    public void understandRangeAPI(){

        //Integer i = new Integer(200)

        Byte b = (byte) 111;
        assertEquals( 111, Byte.toUnsignedInt(b));

        ByteBuffer rawKey = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        rawKey.putLong(12345).putInt(123);


        ByteBuffer geoTempKey = ByteBuffer.allocate(11);

        String hash = GeoHash.encodeHash(23.0,42.0,8);

        geoTempKey.put((byte) 111).putShort((short) 321).put(hash.getBytes());

        Range r  = new Range(new Text(geoTempKey.array()),new Text(geoTempKey.array()));

        Key k = new Key(new Text(geoTempKey.array()),new Text(rawKey.array()));

        assertTrue(r.contains(k));

    }
}

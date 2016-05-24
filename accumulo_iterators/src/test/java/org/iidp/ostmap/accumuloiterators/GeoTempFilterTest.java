package org.iidp.ostmap.accumuloiterators;


import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GeoTempFilterTest {

    @Test
    public void testGeoTempFilter(){

        IteratorSetting filterIteratorConfig = new IteratorSetting(20, "GeoTempFilterIterator", GeoTempFilter.class);
        GeoTempFilter.setBoundingBox(filterIteratorConfig,50,40,
                30,10,10,1000);
        GeoTempFilter gtf = new GeoTempFilter();

        try {
            gtf.init(null,filterIteratorConfig.getOptions(),null);
        } catch (IOException e) {
            //e.printStackTrace();
        }


        ByteBuffer row = ByteBuffer.allocate(11);
        row.put((byte)111);

        ByteBuffer cf = ByteBuffer.allocate(12);
        cf.putLong(100).putInt(2);

        ByteBuffer cq = ByteBuffer.allocate(8);
        cq.putFloat(35).putFloat(35);


        Key k = new Key(new Text(row.array()),new Text(cf.array()),new Text(cq.array()));

        assertTrue(gtf.accept(k, new Value()));

        cq = ByteBuffer.allocate(8);
        cq.putFloat(80).putFloat(35);

        k = new Key(new Text(row.array()),new Text(cf.array()),new Text(cq.array()));

        assertFalse(gtf.accept(k, new Value()));


    }
}

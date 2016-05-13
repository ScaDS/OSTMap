package org.iidp.ostmap.batch_processing.tweetPerUserAnalysis;


import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class TimeFilterTest {

    @Test
    public void testTimeFilter() throws Exception{
        TimeFilter tfAll = new TimeFilter(Long.MIN_VALUE,Long.MAX_VALUE);
        TimeFilter tf = new TimeFilter(0,1000);
        TimeFilter tfNone = new TimeFilter(5,0);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES +Integer.BYTES);
        bb.putLong(100).putInt(123);
        Key k1 = new Key(new Text(bb.array()));

        ByteBuffer bb2 = ByteBuffer.allocate(Long.BYTES +Integer.BYTES);
        bb2.putLong(2000).putInt(1234);
        Key k2 = new Key(new Text(bb2.array()));

        DataSet<Tuple2<Key,Value>> data = env.fromElements(new Tuple2<>(k1, new Value()),
                new Tuple2<>(k2, new Value()));

        assertEquals(2,data.count());
        assertEquals(2,data.filter(tfAll).count());
        assertEquals(1,data.filter(tf).count());
        assertEquals(0,data.filter(tfNone).count());

    }
}

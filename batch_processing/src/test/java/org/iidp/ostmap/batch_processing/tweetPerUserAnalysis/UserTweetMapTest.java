package org.iidp.ostmap.batch_processing.tweetPerUserAnalysis;


import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UserTweetMapTest {

    @Test
    public void testUserTweetMap() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSet<Tuple2<Key,Value>> data = env.fromElements(
                new Tuple2<>(new Key(), new Value((new Text("{\"text\":\"example tweet\"," +
                        "\"user\":{\"screen_name\":\"user1\", \"name\":\"u1name\"}} ")).getBytes())),
                new Tuple2<>(new Key(),  new Value((new Text("{\"text\":\"example tweet\"," +
                        "\"user\":{\"screen_name\":\"user1\", \"name\":\"u1name\"}} ")).getBytes())),
                new Tuple2<>(new Key(), new Value((new Text("{\"text\":\"example tweet\"," +
                        "\"user\":{\"screen_name\":\"user2\", \"name\":\"u1name2\"}} ")).getBytes())));

        DataSet<Tuple2<String,Integer>> out= data.map(new UserTweetMap());

        //System.out.println("result:");
        out.print();

        assertEquals(3,out.count());


    }
}

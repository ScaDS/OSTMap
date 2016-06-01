package org.iidp.ostmap.batch_processing.converter;


import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.Text;
import org.iidp.ostmap.commons.tokenizer.Tokenizer;
import org.junit.Test;


import static org.junit.Assert.assertEquals;

public class ConverterFlatMapTest {

    @Test
    public void testFlatMap() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Key,Value>> input = env.fromElements(
                new Tuple2<>(new Key("row1","t",""), new Value(("{\"text\":\"example tweet\"," +
                        "\"user\":{\"screen_name\":\"user1\", \"name\":\"u1name\"}} ").getBytes())));

        DataSet<Tuple2<Text,Mutation>> output = input.flatMap(
                new ConverterFlatMap(new Tokenizer(),"table"));



        output.print();
        assertEquals(4, output.count());

    }
}

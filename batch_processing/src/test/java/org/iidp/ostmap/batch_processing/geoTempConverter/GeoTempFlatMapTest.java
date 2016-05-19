package org.iidp.ostmap.batch_processing.geoTempConverter;


import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.Text;
import org.iidp.ostmap.batch_processing.converter.ConverterFlatMap;
import org.iidp.ostmap.commons.tokenizer.Tokenizer;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class GeoTempFlatMapTest {
    @Test
    public void testFlatMap() throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ByteBuffer key1 = ByteBuffer.allocate(12);
        key1.putLong(1459350458).putInt(123);
        DataSet<Tuple2<Key,Value>> input = env.fromElements(
                new Tuple2<>(new Key(new Text(key1.array()),new Text("t"),new Text("")),
                        new Value(("{\n" +
                                "  \"created_at\": \"Wed Mar 30 15:07:38 +0000 2016\",\n" +
                                "  \"id\": 715193777833582592,\n" +
                                "  \"user\": {\n" +
                                "    \"id\": 2243967693,\n" +
                                "    \"id_str\": \"2243967693\",\n" +
                                "  },\n" +
                                "  \"geo\": {\n" +
                                "    \"type\": \"Point\",\n" +
                                "    \"coordinates\": [\n" +
                                "      41.00870620,\n" +
                                "      29.21240342\n" +
                                "    ]\n" +
                                "  },\n" +
                                "  \"coordinates\": {\n" +
                                "    \"type\": \"Point\",\n" +
                                "    \"coordinates\": [\n" +
                                "      29.21240342,\n" +
                                "      41.00870620\n" +
                                "    ]\n" +
                                "  },\n" +
                                "  \"place\": {\n" +
                                "    \"id\": \"5e02a0f0d91c76d2\",\n" +
                                "    \"url\": \"https:\\/\\/api.twitter.com\\/1.1\\/geo\\/id\\/5e02a0f0d91c76d2.json\",\n" +
                                "    \"place_type\": \"city\",\n" +
                                "    \"name\": \"\\u0130stanbul\",\n" +
                                "    \"full_name\": \"\\u0130stanbul, T\\u00fcrkiye\",\n" +
                                "    \"country_code\": \"TR\",\n" +
                                "    \"country\": \"T\\u00fcrkiye\",\n" +
                                "    \"bounding_box\": {\n" +
                                "      \"type\": \"Polygon\",\n" +
                                "      \"coordinates\": [\n" +
                                "        [\n" +
                                "          [\n" +
                                "            28.632104,\n" +
                                "            40.802734\n" +
                                "          ],\n" +
                                "          [\n" +
                                "            28.632104,\n" +
                                "            41.239907\n" +
                                "          ],\n" +
                                "          [\n" +
                                "            29.378341,\n" +
                                "            41.239907\n" +
                                "          ],\n" +
                                "          [\n" +
                                "            29.378341,\n" +
                                "            40.802734\n" +
                                "          ]\n" +
                                "        ]\n" +
                                "      ]\n" +
                                "    },\n" +
                                "    \"attributes\": {}\n" +
                                "  },\n" +
                                "  \"timestamp_ms\": \"1459350458950\"\n" +
                                "}\n").getBytes())));

        DataSet<Tuple2<Text,Mutation>> output = input.flatMap(
                new ConverterFlatMap(new Tokenizer(),"table"));



        output.print();
        assertEquals(output.count(), 1);

    }
}

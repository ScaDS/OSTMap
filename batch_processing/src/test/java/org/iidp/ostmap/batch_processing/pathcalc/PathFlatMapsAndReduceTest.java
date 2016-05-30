package org.iidp.ostmap.batch_processing.pathcalc;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.iidp.ostmap.batch_processing.areacalc.Calculator;
import org.iidp.ostmap.batch_processing.areacalc.CoordGroupReduce;
import org.iidp.ostmap.batch_processing.areacalc.GeoCalcFlatMap;
import org.iidp.ostmap.batch_processing.areacalc.GeoExtrationFlatMap;
import org.iidp.ostmap.commons.accumulo.AmcHelper;
import org.iidp.ostmap.commons.enums.TableIdentifier;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.Map;


public class PathFlatMapsAndReduceTest {

    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();
    public static TemporaryFolder tmpSettingsDir = new TemporaryFolder();
    public static AmcHelper amc = new AmcHelper();

    public PathFlatMapsAndReduceTest() throws IOException {
    }

    @BeforeClass
    public static void setUpCluster() {

        amc.startMiniCluster(tmpDir.getRoot().getAbsolutePath());
    }

    @AfterClass
    public static void shutDownCluster() {

        amc.stopMiniCluster();
    }

    @Test
    public void testCalcProcess() throws Exception {
        tmpSettingsDir.create();
        File settings = tmpSettingsDir.newFile("settings");

        Connector conn = amc.getConnector();
        System.out.println("I am connected as: " + conn.whoami());

        conn.tableOperations().create(TableIdentifier.RAW_TWITTER_DATA.get());
        BatchWriter bw = conn.createBatchWriter(TableIdentifier.RAW_TWITTER_DATA.get(), new BatchWriterConfig());


        //write example entries to RawTwitterData
        Mutation m1 = new Mutation("row1");
        m1.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Zorne \n" +
                "   },\n" +
                "   \"timestamp_ms\": 1, \n" +
                "   \"geo\": null, \n" +
                "   \"coordinates\": null, \n" +
                "   \"place\":{\n" +
                "       \"bounding_box\":{\n" +
                "           \"type\":\"Polygon\",\n" +
                "           \"coordinates\":[\n" +
                "               [\n" +
                "                   [\n" +
                "                       -2.164786,\n" +
                "                       52.546974\n" +
                "                   ],\n" +
                "                   [\n" +
                "                       -2.164786,\n" +
                "                       0.546974\n" +
                "                   ],\n" +
                "                   [\n" +
                "                       100.164786,\n" +
                "                       52.546974\n" +
                "                   ],\n" +
                "                   [\n" +
                "                       100.164786,\n" +
                "                       0.546974\n" +
                "                   ]\n" +
                "               ]\n" +
                "           ]\n" +
                "       }, \n" +
                "       \"attributes\":{\n" +
                "           \n" +
                "       }\n" +
                "    }\n" +
                " }");
        bw.addMutation(m1);
        Mutation m2 = new Mutation("row2");
        m2.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Zorne \n" +
                "   },\n" +
                "   \"geo\": null,\n" +
                "   \"timestamp_ms\": 2, \n" +
                "    \"coordinates\": {\n" +
                "      \"type\": \"Point\",\n" +
                "      \"coordinates\": [\n" +
                "        27.2147884,\n" +
                "        38.4614716\n" +
                "      ]\n" +
                "    },\n" +
                "   \"place\": null\n" +
                " }");
        bw.addMutation(m2);
        Mutation m3 = new Mutation("row3");
        m3.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Zorne \n" +
                "   },\n" +
                "   \"geo\": null,\n" +
                "   \"timestamp_ms\": 3, \n" +
                "    \"coordinates\": {\n" +
                "      \"type\": \"Point\",\n" +
                "      \"coordinates\": [\n" +
                "        -0.2147884,\n" +
                "        -0.4614716\n" +
                "      ]\n" +
                "    },\n" +
                "   \"place\": null\n" +
                " }");
        bw.addMutation(m3);
        Mutation m4 = new Mutation("row4");
        m4.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Oliver \n" +
                "   },\n" +
                "   \"geo\": null,\n" +
                "   \"timestamp_ms\": 4, \n" +
                "    \"coordinates\": {\n" +
                "      \"type\": \"Point\",\n" +
                "      \"coordinates\": [\n" +
                "        -3.2147884,\n" +
                "        53.4614716\n" +
                "      ]\n" +
                "    },\n" +
                "   \"place\": null\n" +
                " }");
        bw.addMutation(m4);
        Mutation m5 = new Mutation("row5");
        m5.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Oliver \n" +
                "   },\n" +
                "   \"geo\": null,\n" +
                "   \"timestamp_ms\": 5, \n" +
                "    \"coordinates\": {\n" +
                "      \"type\": \"Point\",\n" +
                "      \"coordinates\": [\n" +
                "        -4.2147884,\n" +
                "        54.4614716\n" +
                "      ]\n" +
                "    },\n" +
                "   \"place\": null\n" +
                " }");
        bw.addMutation(m5);
        Mutation m6 = new Mutation("row6");
        m6.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Oliver \n" +
                "   },\n" +
                "   \"geo\": null,\n" +
                "   \"timestamp_ms\": 6, \n" +
                "    \"coordinates\": {\n" +
                "      \"type\": \"Point\",\n" +
                "      \"coordinates\": [\n" +
                "        -5.2147884,\n" +
                "        55.4614716\n" +
                "      ]\n" +
                "    },\n" +
                "   \"place\": null\n" +
                " }");
        bw.addMutation(m6);
        Mutation m7 = new Mutation("row7");
        m7.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Oliver \n" +
                "   },\n" +
                "   \"geo\": null,\n" +
                "   \"timestamp_ms\": 7, \n" +
                "    \"coordinates\": {\n" +
                "      \"type\": \"Point\",\n" +
                "      \"coordinates\": [\n" +
                "        -2.2147884,\n" +
                "        52.4614716\n" +
                "      ]\n" +
                "    },\n" +
                "   \"place\": null\n" +
                " }");
        bw.addMutation(m7);
        Mutation m8 = new Mutation("row8");
        m8.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Oliver \n" +
                "   },\n" +
                "   \"geo\": null,\n" +
                "   \"timestamp_ms\": 8, \n" +
                "    \"coordinates\": {\n" +
                "      \"type\": \"Point\",\n" +
                "      \"coordinates\": [\n" +
                "        -8.2147884,\n" +
                "        53.4614716\n" +
                "      ]\n" +
                "    },\n" +
                "   \"place\": null\n" +
                " }");
        bw.addMutation(m8);
        Mutation m9 = new Mutation("row9");
        m9.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Peter \n" +
                "   },\n" +
                "   \"geo\": null,\n" +
                "   \"timestamp_ms\": 9, \n" +
                "    \"coordinates\": {\n" +
                "      \"type\": \"Point\",\n" +
                "      \"coordinates\": [\n" +
                "        -3.2147884,\n" +
                "        53.4614716\n" +
                "      ]\n" +
                "    },\n" +
                "   \"place\": null\n" +
                " }");
        bw.addMutation(m9);
        Mutation m10 = new Mutation("row10");
        m10.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Peter \n" +
                "   },\n" +
                "   \"geo\": null,\n" +
                "   \"timestamp_ms\": 10, \n" +
                "    \"coordinates\": {\n" +
                "      \"type\": \"Point\",\n" +
                "      \"coordinates\": [\n" +
                "        47.2147884,\n" +
                "        28.4614716\n" +
                "      ]\n" +
                "    },\n" +
                "   \"place\": null\n" +
                " }");
        bw.addMutation(m10);
        Mutation m11 = new Mutation("row11");
        m11.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Falk \n" +
                "   },\n" +
                "   \"geo\": null,\n" +
                "   \"timestamp_ms\": 11, \n" +
                "    \"coordinates\": {\n" +
                "      \"type\": \"Point\",\n" +
                "      \"coordinates\": [\n" +
                "        0,\n" +
                "        0\n" +
                "      ]\n" +
                "    },\n" +
                "   \"place\": null\n" +
                " }");
        bw.addMutation(m11);
        Mutation m12 = new Mutation("row12");
        m12.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Falk \n" +
                "   },\n" +
                "   \"geo\": null,\n" +
                "   \"timestamp_ms\": 12, \n" +
                "    \"coordinates\": {\n" +
                "      \"type\": \"Point\",\n" +
                "      \"coordinates\": [\n" +
                "        20,\n" +
                "        0\n" +
                "      ]\n" +
                "    },\n" +
                "   \"place\": null\n" +
                " }");
        bw.addMutation(m12);
        Mutation m13 = new Mutation("row13");
        m13.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Falk \n" +
                "   },\n" +
                "   \"geo\": null,\n" +
                "   \"timestamp_ms\": 13, \n" +
                "    \"coordinates\": {\n" +
                "      \"type\": \"Point\",\n" +
                "      \"coordinates\": [\n" +
                "        10,\n" +
                "        20\n" +
                "      ]\n" +
                "    },\n" +
                "   \"place\": null\n" +
                " }");
        bw.addMutation(m13);
        Mutation m14 = new Mutation("row14");
        m14.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Falk \n" +
                "   },\n" +
                "   \"geo\": null,\n" +
                "   \"timestamp_ms\": 14, \n" +
                "    \"coordinates\": {\n" +
                "      \"type\": \"Point\",\n" +
                "      \"coordinates\": [\n" +
                "        10,\n" +
                "        10\n" +
                "      ]\n" +
                "    },\n" +
                "   \"place\": null\n" +
                " }");
        bw.addMutation(m14);
        Mutation m15 = new Mutation("row15");
        m15.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Falk \n" +
                "   },\n" +
                "   \"geo\": null,\n" +
                "   \"timestamp_ms\": 15, \n" +
                "    \"coordinates\": {\n" +
                "      \"type\": \"Point\",\n" +
                "      \"coordinates\": [\n" +
                "        10,\n" +
                "        10\n" +
                "      ]\n" +
                "    },\n" +
                "   \"place\": null\n" +
                " }");
        bw.addMutation(m15);
        Mutation m16 = new Mutation("row16");
        m16.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Falk \n" +
                "   },\n" +
                "   \"geo\": null,\n" +
                "   \"timestamp_ms\": 16, \n" +
                "    \"coordinates\": {\n" +
                "      \"type\": \"Point\",\n" +
                "      \"coordinates\": [\n" +
                "        10,\n" +
                "        10\n" +
                "      ]\n" +
                "    },\n" +
                "   \"place\": null\n" +
                " }");
        bw.addMutation(m16);
        Mutation m17 = new Mutation("row17");
        m17.put("CF", "CQ", "{\n" +
                "   \"user\":{\n" +
                "       \"screen_name\": Falk \n" +
                "   },\n" +
                "   \"geo\": null,\n" +
                "   \"timestamp_ms\": 17, \n" +
                "    \"coordinates\": {\n" +
                "      \"type\": \"Point\",\n" +
                "      \"coordinates\": [\n" +
                "        10,\n" +
                "        10\n" +
                "      ]\n" +
                "    },\n" +
                "   \"place\": null\n" +
                " }");
        bw.addMutation(m17);
        bw.close();

        //output result after conversion
        System.out.println("RawTwitterData: -----------------------------------------------------");
        Scanner s = conn.createScanner(TableIdentifier.RAW_TWITTER_DATA.get(), new Authorizations("standard"));
        for (Map.Entry<Key, Value> entry : s) {
            System.out.println(entry.getKey() + " | " + entry.getValue());
        }
        s.close();

        //create settings file with data of Mini Accumulo Cluster
        FileOutputStream fos = new FileOutputStream(settings, false);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fos));
        String parameters = "accumulo.instance=" + conn.getInstance().getInstanceName() + "\n"+
                "accumulo.user=" + conn.whoami() +"\n"+
                "accumulo.password=password\n"+
                "accumulo.zookeeper=" + conn.getInstance().getZooKeepers();

        System.out.println(parameters);
        br.write(parameters);
        br.flush();
        br.close();
        fos.flush();
        fos.close();

        Calculator calc = new Calculator();
        calc.readConfig(settings.getAbsolutePath());
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Key, Value>> rawData = calc.getDataFromAccumulo(env);


        DataSet<Tuple2<String, String>> geoList = rawData.flatMap(new PathGeoExtrationFlatMap());
        System.out.println("Extracted Data: -----------------------------------------------------");
        geoList.print();

        DataSet<Tuple2<String, String>> reducedGroup = geoList
                .groupBy(0)
                .reduceGroup(new PathCoordGroupReduce());
        System.out.println("Reduced Data: -----------------------------------------------------");
        reducedGroup.print();

        DataSet<Tuple3<String, Double,Integer>> ranking = reducedGroup.flatMap(new PathGeoCalcFlatMap());
        System.out.println("Ranking: -----------------------------------------------------");
        ranking.print();
    }
}
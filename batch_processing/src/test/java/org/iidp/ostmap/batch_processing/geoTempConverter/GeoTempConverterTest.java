package org.iidp.ostmap.batch_processing.geoTempConverter;


import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.iidp.ostmap.batch_processing.converter.ConverterFlatMap;
import org.iidp.ostmap.batch_processing.converter.ConverterProcess;
import org.iidp.ostmap.commons.accumulo.AmcHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class GeoTempConverterTest {

    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();
    public static TemporaryFolder tmpSettingsDir = new TemporaryFolder();
    public static AmcHelper amc = new AmcHelper();

    public GeoTempConverterTest() throws IOException {
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
    public void testConverterProcess() throws Exception {
        tmpSettingsDir.create();
        File settings = tmpSettingsDir.newFile("settings");

        Connector conn = amc.getConnector();
        System.out.println("I am connected as: " + conn.whoami());

        conn.tableOperations().create("RawTwitterData");
        conn.tableOperations().create("GeoTemporalIndex");

        ByteBuffer bb = ByteBuffer.allocate(12);
        bb.putLong(1459350458).putInt(123);

        //write example entry to RawTwitterData
        Mutation m1 = new Mutation(bb.array());
        m1.put("CF", "CQ", "{\n" +
                "  \"created_at\": \"Wed Mar 30 15:07:38 +0000 2016\",\n" +
                "  \"id\": 715193777833582592,\n" +
                "  \"id_str\": \"715193777833582592\",\n" +
                "  \"text\": \"\\u00d6mr\\u00fcm hastanelerde ge\\u00e7iyor (@ \\u00d6zel Sancaktepe B\\u00f6lge Hastanesi in \\u0130stanbul, T\\u00fcrkiye) https:\\/\\/t.co\\/iEwG92qFuc\",\n" +
                "  \"source\": \"\\u003ca href=\\\"http:\\/\\/foursquare.com\\\" rel=\\\"nofollow\\\"\\u003eFoursquare\\u003c\\/a\\u003e\",\n" +
                "  \"truncated\": false,\n" +
                "  \"user\": {\n" +
                "    \"id\": 2243967693,\n" +
                "    \"id_str\": \"2243967693\",\n" +
                "    \"name\": \"Yunus Ya\\u015fin\",\n" +
                "    \"screen_name\": \"uayunus72\",\n" +
                "    \"location\": \"\\u0130stanbul, T\\u00fcrkiye\",\n" +
                "    \"url\": \"http:\\/\\/instagram.com\\/yunusyasn\",\n" +
                "    \"geo_enabled\": true,\n" +
                "    \"lang\": \"tr\",\n" +
                "    \"contributors_enabled\": false,\n" +
                "    \"is_translator\": false,\n" +
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
                "  \"contributors\": null,\n" +
                "  \"is_quote_status\": false,\n" +
                "  \"retweet_count\": 0,\n" +
                "  \"favorite_count\": 0,\n" +
                "  \"entities\": {\n" +
                "    \"hashtags\": [],\n" +
                "    \"urls\": [\n" +
                "      {\n" +
                "        \"url\": \"https:\\/\\/t.co\\/iEwG92qFuc\",\n" +
                "        \"expanded_url\": \"https:\\/\\/www.swarmapp.com\\/c\\/d0LwAoRxhul\",\n" +
                "        \"display_url\": \"swarmapp.com\\/c\\/d0LwAoRxhul\",\n" +
                "        \"indices\": [\n" +
                "          84,\n" +
                "          107\n" +
                "        ]\n" +
                "      }\n" +
                "    ],\n" +
                "    \"user_mentions\": [],\n" +
                "    \"symbols\": []\n" +
                "  },\n" +
                "  \"favorited\": false,\n" +
                "  \"retweeted\": false,\n" +
                "  \"possibly_sensitive\": false,\n" +
                "  \"filter_level\": \"low\",\n" +
                "  \"lang\": \"tr\",\n" +
                "  \"timestamp_ms\": \"1459350458950\"\n" +
                "}\n");
        BatchWriter bw = conn.createBatchWriter("RawTwitterData", new BatchWriterConfig());
        bw.addMutation(m1);
        bw.close();

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

        //run converter
        GeoTempConverter gtc = new GeoTempConverter();
        System.out.println("settings file path: "+settings.getAbsolutePath());
        gtc.run(settings.getAbsolutePath());

        //output result after conversion
        System.out.println("RawTwitterData: -----------------------------------------------------");
        Scanner s = conn.createScanner("RawTwitterData", new Authorizations("standard"));
        for(Map.Entry<Key, Value> entry: s){
            System.out.println(entry.getKey() + " | " +entry.getValue());
            //assertEquals(entry.getValue().toString(), testString);
        }
        s.close();

        System.out.println("GeoTemporalIndex: -----------------------------------------------------");
        s = conn.createScanner("GeoTemporalIndex", new Authorizations("standard"));
        int i = 0;
        for(Map.Entry<Key, Value> entry: s){
            System.out.println(entry.getKey() + " | " + entry.getValue());
            i++;
            //TODO System.out.println(entry.getKey().g);
        }
        s.close();


        System.out.println("TermIndex has "+i+" entrys");
        assertEquals(1, i);

    }
}

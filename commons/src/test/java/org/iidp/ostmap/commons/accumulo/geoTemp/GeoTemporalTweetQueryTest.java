package org.iidp.ostmap.commons.accumulo.geoTemp;

import com.github.davidmoten.geo.Coverage;
import com.github.davidmoten.geo.GeoHash;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.Text;
import org.iidp.ostmap.commons.accumulo.AmcHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GeoTemporalTweetQueryTest implements TweetCallback {

    private String result = "";

    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();
    public static TemporaryFolder tmpSettingsDir = new TemporaryFolder();
    public static AmcHelper amc;
    public static File settings;
    public static String tweetKatze;
    public static String tweetHund;
    public static String tweet3;


    @BeforeClass
    public static void setUpCluster() throws IOException, AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException  {
        amc = new AmcHelper();

        amc.startMiniCluster(tmpDir.getRoot().getAbsolutePath());

        tmpSettingsDir.create();
        settings = tmpSettingsDir.newFile("settings");

        Connector conn = amc.getConnector();
        System.out.println("I am connected as: " + conn.whoami());

        if(!conn.tableOperations().exists("RawTwitterData")){
            conn.tableOperations().create("RawTwitterData");
        }
        if(!conn.tableOperations().exists("GeoTemporalIndex")){
            conn.tableOperations().create("GeoTemporalIndex");
        }

        //write example entry to RawTwitterData
        tweetHund = "{\"text\": \"Vollstaendiger Tweet hund maus\"}";
        tweetKatze = "{\"text\": \"Vollstaendiger Tweet katze #katze maus\"}";
        //File f = new File(AccumuloServiceTest.class.getResource("example-response.json").getFile());
        //File f = new File(ClassLoader.getSystemClassLoader().getResource("example-response.json").getFile());

        tweet3 =  "tweet3";//=  new String(Files.readAllBytes(f.toPath()));

        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        bb.putLong(12345).putInt(123);

        Mutation m1 = new Mutation(bb.array());
        m1.put("t", "", tweetHund);

        ByteBuffer bb2 = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        bb2.putLong(12347).putInt(678);
        Mutation m2 = new Mutation(bb2.array());
        m2.put("t", "", tweetKatze);

        ByteBuffer bb3 = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        bb3.putLong(12349).putInt(679);
        Mutation m11 = new Mutation(bb3.array());
        m11.put("t", "", tweet3);

        System.out.println("keyFormat: " + new String(bb3.array()));

        BatchWriter bw = conn.createBatchWriter("RawTwitterData", new BatchWriterConfig());
        bw.addMutation(m1);
        bw.addMutation(m2);
        bw.addMutation(m11);
        bw.close();

        //write corresponding GeoTemporalIndex
        float lat1 = 40;
        float lon1 = 40;
        float lat2 = 90;
        float lon2 = 40;
        float lat3 = 90;
        float lon3 = 90;

        ByteBuffer rowKey = ByteBuffer.allocate(11);
        rowKey.put((byte) 123).put((new Text(GeoHash.encodeHash(lat1,lon1,8)).getBytes()));
        ByteBuffer cf = ByteBuffer.allocate(8);
        cf.putFloat(lat1).putFloat(lon1);
        Mutation m3 = new Mutation(rowKey.array());
        m3.put(bb.array(), cf.array(), new byte[0]);

        rowKey = ByteBuffer.allocate(11);
        rowKey.put((byte) 122).put((new Text(GeoHash.encodeHash(lat2,lon2,8)).getBytes()));
        cf = ByteBuffer.allocate(8);
        cf.putFloat(lat2).putFloat(lon2);
        Mutation m4 = new Mutation(rowKey.array());
        m4.put(bb2.array(), cf.array(), new byte[0]);

        rowKey = ByteBuffer.allocate(11);
        rowKey.put((byte) 122).put((new Text(GeoHash.encodeHash(lat3,lon3,8)).getBytes()));
        cf = ByteBuffer.allocate(8);
        cf.putFloat(lat3).putFloat(lon3);
        Mutation m5 = new Mutation(rowKey.array());
        m5.put(bb3.array(), cf.array(), new byte[0]);


        System.out.println(Arrays.toString("text".getBytes()));

        BatchWriter bwti = conn.createBatchWriter("GeoTemporalIndex", new BatchWriterConfig());
        bwti.addMutation(m3);
        bwti.addMutation(m4);
        bwti.addMutation(m5);
        bwti.close();

        //create settings file with data of Mini Accumulo Cluster
        FileOutputStream fos = new FileOutputStream(settings, false);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fos));
        String parameters = "accumulo.instance=" + conn.getInstance().getInstanceName() + "\n" +
                "accumulo.user=" + conn.whoami() + "\n" +
                "accumulo.password=password\n" +
                "accumulo.zookeeper=" + conn.getInstance().getZooKeepers();

        System.out.println(parameters);
        br.write(parameters);
        br.flush();
        br.close();
        fos.flush();
        fos.close();


        System.out.println("RawTwitterData: -----------------------------------------------------");
        Scanner s = conn.createScanner("RawTwitterData", new Authorizations("standard"));
        for (Map.Entry<Key, Value> entry : s) {
            System.out.println(entry.getKey() + " | " + entry.getValue());
            //assertEquals(entry.getValue().toString(), testString);
        }
        s.close();
        System.out.println("---------------------------------------------");

        System.out.println("GeoTemporalIndex: -----------------------------------------------------");
        s = conn.createScanner("GeoTemporalIndex", new Authorizations("standard"));
        for (Map.Entry<Key, Value> entry : s) {
            System.out.println(entry.getKey() + " | " + entry.getValue());
            //assertEquals(entry.getValue().toString(), testString);
        }
        s.close();
        System.out.println("---------------------------------------------");

    }


    @AfterClass
    public static void shutDownCluster() {

        amc.stopMiniCluster();
    }

    @Test
    public void testGeoTemporalQuery() throws IOException, AccumuloSecurityException, AccumuloException, TableNotFoundException {

        GeoTemporalTweetQuery gttq = new GeoTemporalTweetQuery(settings.getAbsolutePath());

        gttq.setBoundingBox(60,50,30,20);
        gttq.setTimeRange(12340, 12350);
        gttq.setCallback(this);

        //gttq.query();

        System.out.println(result);
    }

    @Test
    public void testGeoHash(){
        Coverage coverage = GeoHash.coverBoundingBox(60,0,0,40,8);

        System.out.println("getHashes");
        Set<String> hashes = coverage.getHashes();
    }


    @Override
    public void process(String json) {
        result += json;
    }
}

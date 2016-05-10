package org.iidp.ostmap.rest_service;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.iidp.ostmap.commons.accumulo.AmcHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class AccumuloServiceTest {

    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();
    public static TemporaryFolder tmpSettingsDir = new TemporaryFolder();
    public static AmcHelper amc;

    @BeforeClass
    public static void setUpCluster() {
        amc = new AmcHelper();
        amc.startMiniCluster(tmpDir.getRoot().getAbsolutePath());
    }

    @AfterClass
    public static void shutDownCluster() {

        amc.stopMiniCluster();
    }

    @Test
    public void testAccumuloServiceTokenSearch() throws Exception {
        tmpSettingsDir.create();
        File settings = tmpSettingsDir.newFile("settings");

        Connector conn = amc.getConnector();
        System.out.println("I am connected as: " + conn.whoami());

        if(!conn.tableOperations().exists("RawTwitterData")){
            conn.tableOperations().create("RawTwitterData");
        }
        if(!conn.tableOperations().exists("TermIndex")){
            conn.tableOperations().create("TermIndex");
        }

        //write example entry to RawTwitterData
        String tweetHund = "Vollstaendiger Tweet hund";
        String tweetKatze = "Vollstaendiger Tweet katze";

        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        bb.putLong(12345).putInt(123);

        Mutation m1 = new Mutation(bb.array()); //TODO: mit byte[] key testen
        m1.put("t", "", tweetHund);

        ByteBuffer bb2 = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        bb2.putLong(67891).putInt(678);
        Mutation m2 = new Mutation(bb2.array());
        m2.put("t", "", tweetKatze);

        BatchWriter bw = conn.createBatchWriter("RawTwitterData", new BatchWriterConfig());
        bw.addMutation(m1);
        bw.addMutation(m2);
        bw.close();

        //write example data to TermIndex
        Mutation m3 = new Mutation("Vollstaendiger");
        m3.put("text".getBytes(),bb.array(),"2".getBytes());
        Mutation m4 = new Mutation("Tweet");
        m4.put("text".getBytes(),bb.array(),"2".getBytes());
        Mutation m5 = new Mutation("hund");
        m5.put("text".getBytes(),bb.array(),"2".getBytes());
        Mutation m6 = new Mutation("Vollstaendiger");
        m6.put("text".getBytes(),bb2.array(),"2".getBytes());
        Mutation m7 = new Mutation("Tweet");
        m7.put("text".getBytes(),bb2.array(),"2".getBytes());
        Mutation m8 = new Mutation("katze");
        m8.put("text".getBytes(),bb2.array(),"2".getBytes());

        System.out.println(Arrays.toString("text".getBytes()));

        BatchWriter bwti = conn.createBatchWriter("TermIndex", new BatchWriterConfig());
        bwti.addMutation(m3);
        bwti.addMutation(m4);
        bwti.addMutation(m5);
        bwti.addMutation(m6);
        bwti.addMutation(m7);
        bwti.addMutation(m8);
        bwti.close();

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

        //run Token Search
        AccumuloService accumuloService = new AccumuloService();
        System.out.println("settings file path: " + settings.getAbsolutePath());
        accumuloService.readConfig(settings.getAbsolutePath());

        String[] fieldArray = {"user","text"};
        String searchToken = "katze";

        TokenSearchController tsc = new TokenSearchController();

        String result = tsc.getResult(accumuloService, fieldArray,searchToken);


        System.out.println(result + " <-> " + tweetKatze);
        assertEquals(tweetKatze,result);
    }

    @Test
    public void testBuildPrefixFromLong() throws Exception{
        Date date = new Date();
        long longTime = date.getTime();
        byte[] result = AccumuloService.buildPrefix(longTime);

        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
        bb.putLong(longTime);

        System.out.println("testBuildPrefixFromLong");
        //assertEquals(bb.array(),result);
        assertTrue(Arrays.equals(bb.array(),result));
    }

    @Test
    public void testBuildPrefixFromString() throws Exception{
        Date date = new Date();
        long longTime = date.getTime();
        String longString = Long.toString(longTime);
        byte[] result = AccumuloService.buildPrefix(longString);

        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
        bb.putLong(longTime);

        System.out.println("testBuildPrefixFromString");
        //assertEquals(bb.array(),result);
        assertTrue(Arrays.equals(bb.array(),result));
    }


    @Test
    public void testAccumuloServiceTimeGeoSearch() throws Exception {
        tmpSettingsDir.create();
        File settings = tmpSettingsDir.newFile("settings");

        Connector conn = amc.getConnector();
        System.out.println("I am connected as: " + conn.whoami());


        if(!conn.tableOperations().exists("RawTwitterData")){
            conn.tableOperations().create("RawTwitterData");
        }

        //write example entry to RawTwitterData
        String tweetHund = "Vollstaendiger Tweet hund";
        String tweetKatze = "Vollstaendiger Tweet katze";

        Mutation m1 = new Mutation("1462787131_AFC");
        m1.put("t", "", tweetHund);
        Mutation m2 = new Mutation("1462787141_AFC");
        m2.put("t", "", tweetKatze);
        Mutation m3 = new Mutation("1462787151_AFC");
        m3.put("t", "", tweetKatze + tweetHund);

        BatchWriter bw = conn.createBatchWriter("RawTwitterData", new BatchWriterConfig());
        bw.addMutation(m1);
        bw.addMutation(m2);
        bw.addMutation(m3);
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

        //run Timestamp Search
        AccumuloService accumuloService = new AccumuloService();
        System.out.println("settings file path: " + settings.getAbsolutePath());
        accumuloService.readConfig(settings.getAbsolutePath());

        String result = "";
        String startTime = "14627871";
        String endTime = "14627871";

        Scanner rawDataScanner = accumuloService.getRawDataScannerByRange(startTime,endTime);
        for (Map.Entry<Key, Value> rawDataEntry : rawDataScanner) {
            String json = rawDataEntry.getValue().toString();
            result += json;
        }

        System.out.println("Result: " + result);

    }


}

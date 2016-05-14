package org.iidp.ostmap.rest_service;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.iidp.ostmap.commons.accumulo.AmcHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class AccumuloServiceTest {

    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();
    public static TemporaryFolder tmpSettingsDir = new TemporaryFolder();
    public static AmcHelper amc;
    public static File settings;
    public static String tweetKatze;
    public static String tweetHund;
    public static String tweet3;


    @BeforeClass
    public static void setUpCluster() throws IOException, AccumuloException, TableNotFoundException, TableExistsException, AccumuloSecurityException {
        amc = new AmcHelper();
        amc.startMiniCluster(tmpDir.getRoot().getAbsolutePath());

        tmpSettingsDir.create();
        settings = tmpSettingsDir.newFile("settings");

        Connector conn = amc.getConnector();
        System.out.println("I am connected as: " + conn.whoami());

        if(!conn.tableOperations().exists("RawTwitterData")){
            conn.tableOperations().create("RawTwitterData");
        }
        if(!conn.tableOperations().exists("TermIndex")){
            conn.tableOperations().create("TermIndex");
        }

        //write example entry to RawTwitterData
        tweetHund = "Vollstaendiger Tweet hund";
        tweetKatze = "Vollstaendiger Tweet katze #katze";
        //File f = new File(AccumuloServiceTest.class.getResource("example-response.json").getFile());
        File f = new File(ClassLoader.getSystemClassLoader().getResource("example-response.json").getFile());

        tweet3 =  new String(Files.readAllBytes(f.toPath()));

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

        System.out.println("keyFormat: "+new String(bb3.array()));

        BatchWriter bw = conn.createBatchWriter("RawTwitterData", new BatchWriterConfig());
        bw.addMutation(m1);
        bw.addMutation(m2);
        bw.addMutation(m11);
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
        Mutation m9 = new Mutation("#katze");
        m9.put("text".getBytes(),bb2.array(),"2".getBytes());

        System.out.println(Arrays.toString("text".getBytes()));

        BatchWriter bwti = conn.createBatchWriter("TermIndex", new BatchWriterConfig());
        bwti.addMutation(m3);
        bwti.addMutation(m4);
        bwti.addMutation(m5);
        bwti.addMutation(m6);
        bwti.addMutation(m7);
        bwti.addMutation(m8);
        bwti.addMutation(m9);
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


        System.out.println("RawTwitterData: -----------------------------------------------------");
        Scanner s = conn.createScanner("RawTwitterData", new Authorizations("standard"));
        for(Map.Entry<Key, Value> entry: s){
            System.out.println(entry.getKey() + " | " +entry.getValue());
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
    public void testAccumuloServiceTokenSearch() throws Exception {
        //run Token Search
        System.out.println("settings file path: " + settings.getAbsolutePath());

        String fieldList = "user,text";
        String searchToken = "katze";

        TokenSearchController tsc = new TokenSearchController();
        tsc.set_paramCommaSeparatedFieldList(fieldList);
        tsc.set_paramToken(searchToken);
        tsc.validateQueryParams();
        String result = tsc.getResultsFromAccumulo(settings.getAbsolutePath());

        System.out.println(result + " <-> " + "["+tweetKatze+"]");
        assertEquals("["+tweetKatze+"]",result);
    }

    @Test
    public void testAccumuloServiceTokenSearchHashtag() throws Exception {
        //run Token Search
        System.out.println("settings file path: " + settings.getAbsolutePath());

        String fieldList = "user,text";
        String searchToken = "#katze";

        TokenSearchController tsc = new TokenSearchController();
        tsc.set_paramCommaSeparatedFieldList(fieldList);
        tsc.set_paramToken(searchToken);
        tsc.validateQueryParams();
        String result = tsc.getResultsFromAccumulo(settings.getAbsolutePath());

        System.out.println(result + " <-> " + "["+tweetKatze+"]");
        assertEquals("["+tweetKatze+"]",result);
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
     public void testGeoTime() throws Exception{
         AccumuloService accumuloService = new AccumuloService();
         System.out.println("settings file path: " + settings.getAbsolutePath());
         accumuloService.readConfig(settings.getAbsolutePath());

         GeoTimePeriodController gtpc = new GeoTimePeriodController();

         //example dataset should be in this window
         String result = gtpc.getResult(accumuloService,(new Long(12300).toString()),(new Long(12399).toString()),42,30,28,28);
         System.out.println("GeoTimeResult: " + result);
         System.out.println("------");
         assertTrue(result.length() > 2);

         //should not be in time range
         result = gtpc.getResult(accumuloService,(new Long(12399).toString()),(new Long(12400).toString()),42,30,28,28);
         System.out.println("GeoTimeResult: " + result);
         System.out.println("------");
         assertTrue(result.length() == 2);

         //should not be in window
         result = gtpc.getResult(accumuloService,(new Long(12300).toString()),(new Long(12399).toString()),30,45,29,44);
         System.out.println("GeoTimeResult: " + result);
         System.out.println("------");
         assertTrue(result.length() == 2);

/*
         String result = gtpc.getResult(accumuloService,(new Long(12300).toString()),(new Long(12399).toString()));
         System.out.println("GeoTimeResult: " + result);
         assertEquals(tweetHund+tweetKatze+tweet3, result);

         result = gtpc.getResult(accumuloService,(new Long(1).toString()),(new Long(2).toString()));
         System.out.println("GeoTimeResult: " + result);
         assertEquals("", result);

         result = gtpc.getResult(accumuloService,(new Long(12347).toString()),(new Long(12399).toString()));
         System.out.println("GeoTimeResult: " + result);
         assertEquals(tweetKatze+tweet3, result);

         result = gtpc.getResult(accumuloService,(new Long(12340).toString()),(new Long(12348).toString()));
         System.out.println("GeoTimeResult: " + result);
         assertEquals(tweetHund+tweetKatze, result);
*/
     }


}

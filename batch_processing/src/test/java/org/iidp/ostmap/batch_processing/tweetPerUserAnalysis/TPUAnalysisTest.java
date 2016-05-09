package org.iidp.ostmap.batch_processing.tweetPerUserAnalysis;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.iidp.ostmap.batch_processing.ConverterProcess;
import org.iidp.ostmap.commons.accumulo.AmcHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Scanner;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * test for ConverterProcess with example entry in Mini Accumulo Cluster
 */
public class TPUAnalysisTest {

    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();
    public static TemporaryFolder tmpSettingsDir = new TemporaryFolder();
    public static AmcHelper amc = new AmcHelper();

    public TPUAnalysisTest() throws IOException {
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
        File output = tmpSettingsDir.newFile("outputcsv");
        output.delete();

        Connector conn = amc.getConnector();
        System.out.println("I am connected as: " + conn.whoami());

        conn.tableOperations().create("RawTwitterData");
        conn.tableOperations().create("TermIndex");


        //write example entry to RawTwitterData

        BatchWriter bw = conn.createBatchWriter("RawTwitterData", new BatchWriterConfig());

        System.out.println("creating test table");
        for(int i =5; i < 10;i++){
            System.out.println("entry "+i);
            long ts1 = i;
            ByteBuffer bb = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
            bb.putLong(ts1);
            Mutation m1 = new Mutation(bb.array());
            m1.put("CF", "CQ", "{\"text\":\"example tweet"+i+"\"," +
                    "\"user\":{\"screen_name\":\"user1\", \"name\":\"u1name\"}} ");
            bw.addMutation(m1);
        }

        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        bb.putLong(99);
        Mutation m1 = new Mutation(bb.array());
        m1.put("CF", "CQ", "{\"text\":\"example tweet"+7+"\"," +
                "\"user\":{\"screen_name\":\"otherName\", \"name\":\"otherName\"}} ");
        bw.addMutation(m1);

        bw.close();

        System.out.println("creating settings file");

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

        //run TPUAnalysis
        TPUAnalysis tpua = new TPUAnalysis();
        tpua.run(settings.getAbsolutePath(),output.getAbsolutePath(),0,100);

        System.out.println("test table:-----------------------------------------------------");
        org.apache.accumulo.core.client.Scanner s = conn.createScanner("RawTwitterData", new Authorizations("standard"));
        for(Map.Entry<Key, Value> entry: s){
            System.out.println(entry.getKey() + " | " +entry.getValue());
        }
        s.close();


        if(!output.isDirectory()){
            System.out.println("Output file:----------------------------------------------------");
            Scanner in = new Scanner(new FileReader(output.getAbsolutePath()));

            while (in.hasNext()) {
                System.out.println(in.next());
            }
            System.out.println("------------------------------------------------------------------");
        }else{
            System.out.println("Output files:----------------------------------------------------");
            for(File f: output.listFiles()){
                System.out.println(f.getName()+":");
                Scanner in = new Scanner(new FileReader(f));

                while (in.hasNext()) {
                    System.out.println(in.next());
                }
                System.out.println("---------------------");
            }
            System.out.println("------------------------------------------------------------------");
        }



/*
        System.out.println("TermIndex has "+i+" entrys");
        //assertEquals(i, 22);

        d = new ConverterProcess();
        System.out.println("settings file path: "+settings.getAbsolutePath());
        d.run(settings.getAbsolutePath());

        //output result after conversion
        System.out.println("RawTwitterData: -----------------------------------------------------");
        s = conn.createScanner("RawTwitterData", new Authorizations("standard"));
        for(Map.Entry<Key, Value> entry: s){
            System.out.println(entry.getKey() + " | " +entry.getValue());
            //assertEquals(entry.getValue().toString(), testString);
        }
        s.close();

        System.out.println("TermIndex: -----------------------------------------------------");
        s = conn.createScanner("TermIndex", new Authorizations("standard"));
        i = 0;
        for(Map.Entry<Key, Value> entry: s){
            System.out.println(entry.getKey() + " | " + entry.getValue());
            i++;
            if(entry.getKey().getRow().toString().equals("for")){
                //token "for" should appear 3 times
                assertEquals(entry.getValue().toString(),"3");
            }
        }
        s.close();*/

    }


}

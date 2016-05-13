package org.iidp.ostmap.commons.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.iidp.ostmap.stream_processing.Driver;
import org.iidp.ostmap.stream_processing.functions.Tokenizer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Testclass for accumulo's sinks RawTwitterDataSink and TermIndexSink
 * Uses MiniAccumuloCluster to proof that sinks write data in DB
 */


public class SinksTest {


    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();
    private static MiniAccumuloCluster accumulo;

    @BeforeClass
    public static void setUpCluster() throws AccumuloException, AccumuloSecurityException, InterruptedException, IOException {
        accumulo = new MiniAccumuloCluster(tmpDir.getRoot().getAbsoluteFile(), "password");
        accumulo.start();
    }

    @AfterClass
    public static void shutDownCluster() throws IOException, InterruptedException {
        accumulo.stop();
    }

    @Test
    public void testSomething() throws Exception {

        String tweet = "{\"created_at\":\"Fri Apr 29 09:05:45 +0000 2016\",\"id\":725974381906804738,\"id_str\":\"725974381906804738\",\"text\":\"Das sage ich dir gleich, das funktioniert doch nie! #haselnuss\",\"user\":{\"id\":179905182,\"name\":\"Peter Tosh\",\"screen_name\":\"PeTo\",\"location\":null\",\"lang\":\"de\",\"contributors_enabled\":false}}";
        String tweet2 = "{\"created_at\":\"Fri Apr 29 09:05:55 +0000 2016\",\"id\":725974381906804739,\"id_str\":\"725974381906804739\",\"text\":\"Jetzt ist Sommer! #eis\",\"user\":{\"id\":179905182,\"name\":\"Peter Tosh\",\"screen_name\":\"PeTo\",\"location\":null\",\"lang\":\"de\",\"contributors_enabled\":false}}";
        String tweet3 = "{\"created_at\":\"Sat Apr 30 10:09:55 +0000 2016\",\"id\":725974381906804740,\"id_str\":\"725974381906804740\",\"text\":\"Work, work, work? #finished\",\"user\":{\"id\":179905182,\"name\":\"Peter Tosh\",\"screen_name\":\"PeTo\",\"location\":null\",\"lang\":\"en\",\"contributors_enabled\":false}}";
        ArrayList<String> tweetList = new ArrayList<>();
        tweetList.add(tweet);
        tweetList.add(tweet2);
        tweetList.add(tweet3);

        Driver dr = new Driver();
        // we don't need paths to files (for run) because we provide tweet for local stream and want to use minicluster with default properties
        dr.addMACdata(accumulo.getInstanceName(), accumulo.getZooKeepers());
        String path = "/home/loewwnzahn/Dokumente/uni/master_sem02/praxis/workspace/OSTMap/stream_processing/twitter.properties";
        dr.run("", "", tweetList);

        //We have to wait because in the sinks the writers have maxLatency of 10 secs
        Thread.sleep(22000);

        Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
        Connector conn = instance.getConnector("root", new PasswordToken("password"));

        Authorizations auth = new Authorizations("standard");
        conn.securityOperations().changeUserAuthorizations("root", auth);

        //Test for user's name
        Scanner s0 = conn.createScanner("TermIndex", new Authorizations("standard"));
        s0.setRange(new Range("peter tosh", true, "peter tosh", true));
        for(Map.Entry<Key, Value> entry: s0){
            assertEquals(entry.getKey().getColumnFamily().toString(), "user");
            System.out.println("[1/5] Tested user's name with success");
        }
        s0.close();


        //Test for user's screen_name
        Scanner s1 = conn.createScanner("TermIndex", new Authorizations("standard"));
        s1.setRange(new Range("peto", true, "peto", true));
        for(Map.Entry<Key, Value> entry: s1){
            assertEquals(entry.getKey().getColumnFamily().toString(), "user");
            System.out.println("[2/5] Tested user's shown_name with success");
        }
        s1.close();


        //Test for token-count
        Scanner s2 = conn.createScanner("TermIndex", new Authorizations("standard"));
        s2.setRange(new Range("das", true, "das", true));
        for(Map.Entry<Key, Value> entry: s2){
            int countDas = Integer.parseInt(entry.getValue().toString());
            assertTrue(countDas == 2);
            System.out.println("[3/5] Tested term count with success");
        }
        s2.close();

        //Test for tokens
        String text = "Das sage ich dir gleich, das funktioniert doch nie! #haselnuss";
        Tokenizer t = new Tokenizer();
        Scanner s3 = conn.createScanner("TermIndex", new Authorizations("standard"));
        ArrayList<String> list = new ArrayList<String>();
        for(Map.Entry<Key, Value> entry: s3){
            list.add(entry.getKey().getRow().toString());
        }

        System.out.println("[4/5] size of list is " + list.size() + " and we have " + t.tokenizeString(text).size() + " tokens.");


        for(String token : t.tokenizeString(text))
        {
            System.out.println("------Test for: " + token);
            assertTrue(list.contains(token));
        }
        s3.close();

        System.out.println("[5/5] Tested tokens with success");

        Scanner s4 = conn.createScanner("TweetFrequency", new Authorizations("standard"));
        for(Map.Entry<Key, Value> entry: s4){
            System.out.println("--INDB: " + entry.getKey() + "___" + entry.getValue());
        }

    }
}

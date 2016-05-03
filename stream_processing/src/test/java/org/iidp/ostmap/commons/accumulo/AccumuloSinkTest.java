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
 * Testclass for AccumuloSink
 * Uses MiniAccumuloCluster to proof that AS writes data in DB
 * NOTICE: You have to edit paths to config-files below
 */


public class AccumuloSinkTest {


    private String twProp = "/your/path/to/configFiles/twitter.properties";
    private String accProp = "/your/path/to/configFiles/accumulo.properties";

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

        Driver dr = new Driver();
        dr.addMACdata(accumulo.getInstanceName(), accumulo.getZooKeepers());

        String tweet = "{\"created_at\":\"Fri Apr 29 09:05:55 +0000 2016\",\"id\":725974381906804738,\"id_str\":\"725974381906804738\",\"text\":\"Das sage ich dir gleich, das funktioniert doch nie! #haselnuss\",\"user\":{\"id\":179905182,\"name\":\"Peter Tosh\",\"screen_name\":\"PeTo\"}}";
        dr.run(twProp, accProp, tweet);

        //We have to wait because writer in AccumuloSink has maxLatency of 10 secs
        Thread.sleep(11000);

        Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
        Connector conn = instance.getConnector("root", new PasswordToken("password"));

        Authorizations auth = new Authorizations("standard");
        conn.securityOperations().changeUserAuthorizations("root", auth);

        //Test for user
        Scanner s1 = conn.createScanner("TermIndex", new Authorizations("standard"));
        s1.setRange(new Range("Peter Tosh", true, "Peter Tosh", true));
        for(Map.Entry<Key, Value> entry: s1){
            assertEquals(entry.getKey().getColumnFamily().toString(), "user");
        }
        s1.close();


        //Test for token-count
        Scanner s2 = conn.createScanner("TermIndex", new Authorizations("standard"));
        s2.setRange(new Range("das", true, "das", true));
        for(Map.Entry<Key, Value> entry: s2){
            int countDas = Integer.parseInt(entry.getValue().toString());
            assertTrue(countDas == 2);
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

        for(String token : t.tokenizeString(text))
        {
            assertTrue(list.contains(token));
        }
        s3.close();


    }
}

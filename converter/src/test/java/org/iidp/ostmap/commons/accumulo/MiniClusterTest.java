package org.iidp.ostmap.commons.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


import java.io.IOException;
import java.util.Map;

/**
 * Created by hans on 27.04.16.
 */
public class MiniClusterTest {
    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();
    private static MiniAccumuloCluster accumulo;
    //public AmcHelper amc = new AmcHelper();

    @BeforeClass
    public static void setUpCluster() throws AccumuloException, AccumuloSecurityException, InterruptedException, IOException {
        //amc.startMiniCluster(tmpDir.getRoot().getAbsolutePath());
        //File tempDirectory = // JUnit and Guava supply mechanisms for creating temp directories
        accumulo = new MiniAccumuloCluster(tmpDir.getRoot().getAbsoluteFile(), "password");
        accumulo.start();
    }

    @AfterClass
    public static void shutDownCluster() throws IOException, InterruptedException {
        accumulo.stop();
    }

    @Test
    public void testSomething() throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        //Connector connector = amc.getConnector();
        System.out.println("I am connected as: " + accumulo.getInstanceName());

        Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
        Connector conn = instance.getConnector("root", new PasswordToken("password"));
        System.out.println(conn.whoami());

        Authorizations auth = new Authorizations("a");
        conn.securityOperations().changeUserAuthorizations("root", auth);

        conn.tableOperations().create("TestTable");
        Mutation m1 = new Mutation("row1");
        m1.put("CF", "CQ", "42");
        BatchWriter bw = conn.createBatchWriter("TestTable", new BatchWriterConfig());
        bw.addMutation(m1);
        bw.close();

        Scanner s = conn.createScanner("TestTable", new Authorizations("a"));
        for(Map.Entry<Key, Value> entry: s){
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
        }

        s.close();
    }
}

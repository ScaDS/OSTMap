/**
 * Test for AmcHelper
 * saves string to a table and reads it back
 */

package org.iidp.ostmap.commons.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Map;

import static org.junit.Assert.assertEquals;

// further imports
public class AmcHelperTest {
    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();
    public static AmcHelper amc = new AmcHelper();

    @BeforeClass
    public static void setUpCluster() {
        amc.startMiniCluster(tmpDir.getRoot().getAbsolutePath());
    }

    @AfterClass
    public static void shutDownCluster() {
        amc.stopMiniCluster();
    }

    @Test
    public void testAmc() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Connector conn = amc.getConnector();
        System.out.println("I am connected as: " + conn.whoami());

        conn.tableOperations().create("TestTable");
        Mutation m1 = new Mutation("row1");
        String testString = "42";
        m1.put("CF", "CQ", testString);
        BatchWriter bw = conn.createBatchWriter("TestTable", new BatchWriterConfig());
        bw.addMutation(m1);
        bw.close();

        Scanner s = conn.createScanner("TestTable", new Authorizations("a"));
        for(Map.Entry<Key, Value> entry: s){
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
            assertEquals(entry.getValue().toString(), testString);
        }

        s.close();
    }
}
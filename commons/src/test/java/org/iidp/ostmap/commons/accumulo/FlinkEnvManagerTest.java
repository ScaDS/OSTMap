package org.iidp.ostmap.commons.accumulo;


import org.apache.accumulo.core.client.Connector;
import org.iidp.ostmap.commons.enums.TableIdentifier;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class FlinkEnvManagerTest {

    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();
    public static TemporaryFolder tmpSettingsDir = new TemporaryFolder();
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
    public void testFlinkEnvManager() throws Exception{

        tmpSettingsDir.create();
        File settings = tmpSettingsDir.newFile("settings");

        Connector conn = amc.getConnector();

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

        FlinkEnvManager fem = new FlinkEnvManager(settings.getAbsolutePath(),"testJob",
                TableIdentifier.RAW_TWITTER_DATA.get(),
                TableIdentifier.TERM_INDEX.get());

        assertNotNull(fem.getDataFromAccumulo());
        assertNotNull(fem.getExecutionEnvironment());
        assertNotNull(fem.getHadoopOF());


    }
}

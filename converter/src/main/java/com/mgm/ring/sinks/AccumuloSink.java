package com.mgm.ring.sinks;

import com.google.common.io.Files;
import com.mgm.ring.types.CustomKey;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * This class is a sink for Apache Flink Stream API writing to Apache Accumulo.
 * It needs a config file with following content: accumulo.instance, accumulo.user, accumulo.password, accumulo.zookeeper.
 *
 * @author Martin Grimmer (martin.grimmer@mgm-tp.com)
 */
public class AccumuloSink extends RichSinkFunction<Tuple2<CustomKey, String>> {

    private transient BatchWriter writer = null;
    public static final String PROPERTY_INSTANCE = "accumulo.instance";
    private String accumuloInstanceName;
    public static final String PROPERTY_USER = "accumulo.user";
    private String accumuloUser;
    public static final String PROPERTY_PASSWORD = "accumulo.password";
    private String accumuloPassword;
    public static final String PROPERTY_ZOOKEEPER = "accumulo.zookeeper";
    private String accumuloZookeeper;
    private static String zoohostFilePath =  (new File(System.getProperty("java.io.tmpdir"),"mac.tmp")).getAbsolutePath();

    private String table;

    private static final byte[] COLUMN_FAMILY = "t".getBytes();
    private static final byte[] EMPTY_BYTES = new byte[0];

    private static Logger log = Logger.getLogger(AccumuloSink.class);

    /**
     * empty constructor
     */
    public AccumuloSink() {

    }

    /**
     * parses the config file at the given position for the necessary parameter
     *
     * @param path to the config file
     * @throws IOException
     */
    private void readConfig(String path) throws IOException {
        Properties props = new Properties();
        FileInputStream fis = new FileInputStream(path);
        props.load(fis);
        accumuloInstanceName = props.getProperty(PROPERTY_INSTANCE);
        accumuloUser = props.getProperty(PROPERTY_USER);
        accumuloPassword = props.getProperty(PROPERTY_PASSWORD);
        accumuloZookeeper = props.getProperty(PROPERTY_ZOOKEEPER);
    }

    /**
     * creates a batchwriter to write data to accumulo
     *
     * @param table to write data into
     * @return a ready to user batch writer object
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    private BatchWriter createBatchWriter(String table) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        final BatchWriterConfig bwConfig = new BatchWriterConfig();
        // buffer max 100kb ( 100 * 1024 = 102400)
        bwConfig.setMaxMemory(102400);
        // buffer max 10 seconds
        bwConfig.setMaxLatency(10, TimeUnit.SECONDS);
        // ensure persistance
        bwConfig.setDurability(Durability.SYNC);

        // build the accumulo connector connector
        System.out.println(accumuloInstanceName + " "+accumuloZookeeper + "aaabbb");
        Instance inst = new ZooKeeperInstance(accumuloInstanceName, accumuloZookeeper);
        Connector conn = inst.getConnector(accumuloUser, new PasswordToken(accumuloPassword));
        Authorizations auths = new Authorizations("standard");
        conn.securityOperations().changeUserAuthorizations("root", auths);

        // create the table if not already existent
        TableOperations tableOpts = conn.tableOperations();
        if (!tableOpts.exists(table)) {
            tableOpts.create(table);
        }

        // build and return the batchwriter
        return conn.createBatchWriter(table, bwConfig);
    }

    /**
     * configures this instance of AccumuloSink
     *
     * @param configFile file with information needed for accumulo connection
     * @param table      table name to write the data to
     * @throws IOException
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    public void configure(String configFile, String table) throws IOException, InterruptedException,AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {


        /**
        File tempDir = Files.createTempDir();
        tempDir.deleteOnExit();
        MiniAccumuloCluster accumulo = new MiniAccumuloCluster(tempDir, "password");
        accumulo.start();
        Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
        Thread.sleep(3000);

        accumuloInstanceName = accumulo.getInstanceName();
        accumuloUser = "root";
        accumuloPassword = "password";
        accumuloZookeeper = accumulo.getZooKeepers();
        **/
        log.info("configuring accumulo sink with " + configFile + " for " + table);
        //read config file
        readConfig(configFile);
        this.table = table;
    }

    @Override
    /**
     * this is called for each tweet
     */
    public void invoke(Tuple2<CustomKey, String> value) throws Exception {
        // if the writer isnt already instantiated, do it now
        if (writer == null) {
            writer = createBatchWriter(table);
        }

        // build a mutation from the input
        Mutation mutation = new Mutation(value._1().bytes); // the row to write to = customkey.bytes
        mutation.put(COLUMN_FAMILY, EMPTY_BYTES, value._2().getBytes()); // column family, column qualifier and value to write

        // write the mutation
        writer.addMutation(mutation);
    }
}

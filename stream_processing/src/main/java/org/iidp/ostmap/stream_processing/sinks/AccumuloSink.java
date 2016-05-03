package org.iidp.ostmap.stream_processing.sinks;

import org.iidp.ostmap.stream_processing.types.CustomKey;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;
import scala.Tuple3;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * This class is a sink for Apache Flink Stream API writing to Apache Accumulo.
 * It needs a config file with following content: accumulo.instance, accumulo.user, accumulo.password, accumulo.zookeeper.
 */
public class AccumuloSink extends RichSinkFunction<Tuple3<CustomKey, Integer, String>> {

    private BatchWriter writerForRawData = null;
    private BatchWriter writerForTerms = null;
    public static final String PROPERTY_INSTANCE = "accumulo.instance";
    private String accumuloInstanceName;
    public static final String PROPERTY_USER = "accumulo.user";
    private String accumuloUser;
    public static final String PROPERTY_PASSWORD = "accumulo.password";
    private String accumuloPassword;
    public static final String PROPERTY_ZOOKEEPER = "accumulo.zookeeper";
    private String accumuloZookeeper;

    private String tableRawData;
    private String tableTerms;
    private Connector conn;

    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte[] T_BYTES = "t".getBytes();
    private static byte[] lastWrittenKey;

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

        // build the accumulo connector
        Instance inst = new ZooKeeperInstance(accumuloInstanceName, accumuloZookeeper);
        conn = inst.getConnector(accumuloUser, new PasswordToken(accumuloPassword));
        Authorizations auths = new Authorizations("standard");
        conn.securityOperations().changeUserAuthorizations("root", auths);

        // create the table if not already existent
        TableOperations tableOpts = conn.tableOperations();
        try{
            tableOpts.create(table);
        } catch(Exception e) {}

        // build and return the batchwriter
        return conn.createBatchWriter(table, bwConfig);
    }

    /**
     * configures this instance of AccumuloSink
     *
     * @param configFile file with information needed for accumulo connection
     * @param tableRawData      table name to write the data to
     * @param tableTerms      table name to write the data to
     * @throws IOException
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    public void configure(String configFile, String tableTerms, String tableRawData) throws IOException, AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        log.info("configuring accumulo sink with " + configFile + " for " + tableTerms + " and " + tableRawData );
        // read config file
        readConfig(configFile);
        this.tableRawData = tableRawData;
        this.tableTerms = tableTerms;

    }

    /**
     * configures this instance of AccumuloSink
     *
     * @param tableRawData      table name to write the data to
     * @param tableTerms      table name to write the data to
     * @throws IOException
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    public void configure(String tableTerms, String tableRawData, String accumuloInstanceName, String accumuloZookeeper) throws IOException, AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        log.info("configuring accumulo sink for tables " + tableTerms + " and " + tableRawData );
        this.accumuloInstanceName = accumuloInstanceName;
        accumuloUser = "root";
        accumuloPassword = "password";
        this.accumuloZookeeper = accumuloZookeeper;

        this.tableRawData = tableRawData;
        this.tableTerms = tableTerms;
    }

    @Override
    /**
     * this is called for each token in tweet
     * writes rawTwitterData and TermData into the two tables
     */
    public void invoke(Tuple3<CustomKey, Integer, String> value) throws Exception {
        // if the writers arent already instantiated, do it now
        if (writerForTerms == null) {
            writerForTerms = createBatchWriter(tableTerms);
        }
        if (writerForRawData == null) {
            writerForRawData = createBatchWriter(tableRawData);
        }

        // get bytes of relevant data
        byte[] dataType = value._1().type.getBytes();   //defines the type of data (text/user)
        byte[] oldKey = value._1().foreignKeyBytes;  // foreign key to the row in the original table
        byte[] token = value._1().row.getBytes();  // the token for the row
        byte[] tweet = value._3().getBytes();  // the tweet

        //Write raw data once per tweet
        if(lastWrittenKey==null || !Arrays.equals(lastWrittenKey, oldKey)) {
            Mutation mutationRawData = new Mutation(oldKey);
            if (value._2().intValue() == 0 || value._2().intValue() == 1) {
                mutationRawData.put(T_BYTES, EMPTY_BYTES, tweet); // column family, column qualifier and tweet as string
            }
            writerForRawData.addMutation(mutationRawData);
            lastWrittenKey = oldKey;
        }

        //Write termData once per token/user
        Mutation mutationTerms = new Mutation(token);
        if(value._2().intValue()==0 || value._2().intValue()==1)
        {
            //put without value
            mutationTerms.put(dataType, oldKey, EMPTY_BYTES); // column family, column qualifier without value
        }
        else
        {
            //put with number of occurrences
            mutationTerms.put(dataType, oldKey, (""+value._2().intValue()).getBytes()); // column family, column qualifier and value to write
        }
        writerForTerms.addMutation(mutationTerms);
    }
}

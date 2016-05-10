package org.iidp.ostmap.stream_processing.sinks;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;
import org.iidp.ostmap.stream_processing.types.RawTwitterDataKey;
import org.iidp.ostmap.stream_processing.types.SinkConfiguration;
import scala.Tuple2;
import java.util.concurrent.TimeUnit;

/**
 * This class is a sink for Apache Flink Stream API writing to Apache Accumulo.
 * It needs a config file of type SinkConfiguration
 */
public class RawTwitterDataSink extends RichSinkFunction<Tuple2<RawTwitterDataKey, String>> {

    private static Logger log = Logger.getLogger(RawTwitterDataSink.class);
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte[] T_BYTES = "t".getBytes();

    private BatchWriter writerForRawData = null;
    private SinkConfiguration cfg;
    private String tableRawData;
    private Connector conn;

    /**
     * empty constructor
     */
    public RawTwitterDataSink() {

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
        Instance inst = new ZooKeeperInstance(cfg.accumuloInstanceName, cfg.accumuloZookeeper);
        conn = inst.getConnector(cfg.accumuloUser, new PasswordToken(cfg.accumuloPassword));
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
     * configures this instance of RawTwitterDataSink
     *
     * @param sinkConfiguration     the configuration for this sink
     * @param tableRawData            the table to write into
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     * @throws TableExistsException
     */
    public void configure(SinkConfiguration sinkConfiguration, String tableRawData) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        log.info("configuring accumulo sink for " + tableRawData );
        this.cfg = sinkConfiguration;
        this.tableRawData = tableRawData;
    }


    @Override
    /**
     * this is called for each tweet, writes rawTwitterData into db
     */
    public void invoke(Tuple2<RawTwitterDataKey, String> value) throws Exception {
        // if the writer isnt already instantiated, do it now
        if (writerForRawData == null) {
            writerForRawData = createBatchWriter(tableRawData);
        }

        // bytes of the string containing tweet's json
        byte[] tweet = value._2().getBytes();

        //Write raw data once per tweet
        Mutation mutationRawData = new Mutation(value._1.keyBytes);
        // column family, column qualifier and tweet as string
        mutationRawData.put(T_BYTES, EMPTY_BYTES, tweet);

        writerForRawData.addMutation(mutationRawData);
    }
}

package org.iidp.ostmap.stream_processing.sinks;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.Logger;
import org.iidp.ostmap.stream_processing.types.SinkConfiguration;
import org.iidp.ostmap.stream_processing.types.TermIndexKey;
import scala.Tuple2;
import java.util.concurrent.TimeUnit;

/**
 * This class is a sink for Apache Flink Stream API writing to Apache Accumulo.
 * It needs a config of type SinkConfiguration
 */
public class TermIndexSink extends RichSinkFunction<Tuple2<TermIndexKey, Integer>> {

    private BatchWriter writerForTerms = null;

    private String tableTerms;
    private SinkConfiguration cfg;
    private Connector conn;

    private static final byte[] EMPTY_BYTES = new byte[0];

    private static Logger log = Logger.getLogger(TermIndexSink.class);

    /**
     * empty constructor
     */
    public TermIndexSink() {
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
     * configures this instance of TermIndexSink
     *
     * @param sinkConfiguration     the configuration for this sink
     * @param tableTerms            the table to write into
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     * @throws TableExistsException
     */
    public void configure(SinkConfiguration sinkConfiguration, String tableTerms) throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        log.info("configuring accumulo sink for " + tableTerms );
        this.cfg = sinkConfiguration;
        this.tableTerms = tableTerms;
    }

    @Override
    /**
     * this is called for each token in tweet
     */
    public void invoke(Tuple2<TermIndexKey, Integer> value) throws Exception {
        // if the writer isnt already instantiated, do it now
        if (writerForTerms == null) {
            writerForTerms = createBatchWriter(tableTerms);
        }

        //Write termData once per token/user
        Mutation mutationTerms = new Mutation(value._1.getTermBytes());
        if(value._2()<=1)
        {
            //put without value
            // column family, column qualifier without value
            mutationTerms.put(value._1.getSourceBytes(), value._1.rawTwitterDataKey.keyBytes, EMPTY_BYTES);
        }
        else
        {
            //put with number of occurrences
            // column family, column qualifier and value to write
            mutationTerms.put(value._1.getSourceBytes(), value._1.rawTwitterDataKey.keyBytes, (""+value._2()).getBytes());
        }
        writerForTerms.addMutation(mutationTerms);
    }
}

package org.iidp.ostmap.stream_processing.sinks;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.iidp.ostmap.commons.accumulo.geoTemp.GeoTemporalKey;
import org.iidp.ostmap.stream_processing.types.RawTwitterDataKey;
import org.iidp.ostmap.stream_processing.types.SinkConfiguration;
import scala.Tuple2;

import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * This class is a sink for Apache Flink Stream API writing to Apache Accumulo.
 * It needs a config of type SinkConfiguration
 */
public class GeoTemporalIndexSink extends RichSinkFunction<Tuple2<RawTwitterDataKey, GeoTemporalKey>> {

    private BatchWriter writerForGeoTempIndex = null;

    private String tableTerms;
    private SinkConfiguration cfg;
    private Connector conn;

    private static final byte[] EMPTY_BYTES = new byte[0];

    private static Logger log = Logger.getLogger(GeoTemporalIndexSink.class);

    /**
     * empty constructor
     */
    public GeoTemporalIndexSink() {
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
        Authorizations auths = new Authorizations("ostmap");
        //conn.securityOperations().changeUserAuthorizations("root", auths);

        // create the table if not already existent
        TableOperations tableOpts = conn.tableOperations();
        try{
            if(!tableOpts.exists(table)) {
                tableOpts.create(table);
                // create the presplits for the table
                TreeSet<Text> splits = new TreeSet<Text>();
                for (int i = 0; i <= 255; i++) {
                    byte[] bytes = {(byte) i};
                    splits.add(new Text(bytes));
                }
                tableOpts.addSplits(table, splits);
            }
        } catch(Exception e) {
            log.error(e);
        }

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
     * this is called for each tweet
     */
    public void invoke(Tuple2<RawTwitterDataKey, GeoTemporalKey> value) throws Exception {
        // if the writer isnt already instantiated, do it now
        if (writerForGeoTempIndex == null) {
            writerForGeoTempIndex = createBatchWriter(tableTerms);
        }

        //Write geoTemporalIndex once per tweet
        Mutation mutationTerms = new Mutation(value._2.rowBytes);

        mutationTerms.put(value._1.keyBytes, value._2.columQualifier, EMPTY_BYTES);
        writerForGeoTempIndex.addMutation(mutationTerms);
    }
}

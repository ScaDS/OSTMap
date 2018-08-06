package org.iidp.ostmap.stream_processing.sinks;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.io.Text;
import org.iidp.ostmap.commons.enums.AccumuloIdentifiers;
import org.iidp.ostmap.stream_processing.types.SinkConfiguration;
import org.apache.log4j.Logger;

import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class SentimentDataSink extends RichSinkFunction<Tuple3<String, Integer, String>> {

    private static Logger log = Logger.getLogger(SentimentDataSink.class);

    private BatchWriter batchWriter = null;
    private Connector connector;
    private SinkConfiguration sinkConfiguration;
    private static final byte[] EMPTY_BYTES = new byte[0];

    public SentimentDataSink() {
    }

    @Override
    public void invoke(Tuple3<String, Integer, String> value) throws Exception {

    }

    private BatchWriter createBatchWriter(String tableName)
            throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        final BatchWriterConfig batchWriterConfig = new BatchWriterConfig();

        batchWriterConfig.setMaxMemory(102400);
        batchWriterConfig.setMaxLatency(10, TimeUnit.SECONDS);
        batchWriterConfig.setDurability(Durability.SYNC);

        Instance instance = new ZooKeeperInstance(
                sinkConfiguration.accumuloInstanceName, sinkConfiguration.accumuloZookeeper);

        connector = instance.getConnector(
                sinkConfiguration.accumuloUser, new PasswordToken(sinkConfiguration.accumuloPassword));

        Authorizations authorizations = new Authorizations(AccumuloIdentifiers.AUTHORIZATION.toString());

        TableOperations tableOperations = connector.tableOperations();
        try {
            if (!tableOperations.exists(tableName)) {

                tableOperations.create(tableName);
                TreeSet<Text> textTreeSet = new TreeSet<>();

                for (int i = 0; i <= 255; i++) {
                    byte[] bytes = {(byte) i};
                    textTreeSet.add(new Text(bytes));
                }
                tableOperations.addSplits(tableName, textTreeSet);
            }
        } catch (Exception e) {
            log.error(e);
        }

        return connector.createBatchWriter(tableName, batchWriterConfig);
    }
}

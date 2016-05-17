package org.iidp.ostmap.batch_processing.areacalc;


import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.iidp.ostmap.commons.enums.TableIdentifier;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * This class determines the user with the biggest area defined by his tweets
 */
public class Calculator {
    public static final String PROPERTY_INSTANCE = "accumulo.instance";
    private String accumuloInstanceName;
    public static final String PROPERTY_USER = "accumulo.user";
    private String accumuloUser;
    public static final String PROPERTY_PASSWORD = "accumulo.password";
    private String accumuloPassword;
    public static final String PROPERTY_ZOOKEEPER = "accumulo.zookeeper";
    private String accumuloZookeeper;
    public static final String inTable = TableIdentifier.RAW_TWITTER_TABLE.get();
    private Job job;




    /**
     * parses the config file at the given position for the necessary parameters
     *
     * @param path
     * @throws IOException
     */
    //TODO make private again
    public void readConfig(String path) throws IOException {
        Properties props = new Properties();
        FileInputStream fis = new FileInputStream(path);
        props.load(fis);
        accumuloInstanceName = props.getProperty(PROPERTY_INSTANCE);
        accumuloUser = props.getProperty(PROPERTY_USER);
        accumuloPassword = props.getProperty(PROPERTY_PASSWORD);
        accumuloZookeeper = props.getProperty(PROPERTY_ZOOKEEPER);
    }

    /**
     * makes accumulo input accessible by flink DataSet api
     * @param env
     * @return
     * @throws IOException
     * @throws AccumuloSecurityException
     */
    // TODO make private after testing
    public DataSet<Tuple2<Key,Value>> getDataFromAccumulo(ExecutionEnvironment env) throws IOException, AccumuloSecurityException {
        job = Job.getInstance(new Configuration(), "areaCalculationJob");
        AccumuloInputFormat.setConnectorInfo(job, accumuloUser, new PasswordToken(accumuloPassword));
        AccumuloInputFormat.setScanAuthorizations(job, new Authorizations("standard"));
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.withInstance(accumuloInstanceName);
        clientConfig.withZkHosts(accumuloZookeeper);
        AccumuloInputFormat.setZooKeeperInstance(job, clientConfig);
        AccumuloInputFormat.setInputTableName(job, inTable);
        return env.createHadoopInput(new AccumuloInputFormat(),Key.class,Value.class, job);
    }

    /**
     * run area calculation process
     * @param path path to config file
     * @throws Exception
     */
    public void run(String path) throws Exception {

        readConfig(path);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Key,Value>> rawTwitterDataRows = getDataFromAccumulo(env);

        DataSet<Tuple2<String,String>> geoList = rawTwitterDataRows.flatMap(new GeoExtrationFlatMap());

        DataSet<Tuple2<String,String>> reducedGroup = geoList
                                                        .groupBy(0)
                                                        .reduceGroup(new CoordGroupReduce());

        DataSet<Tuple2<String,Double>> userRanking = reducedGroup.flatMap(new GeoCalcFlatMap())
                .sortPartition(1, Order.DESCENDING).setParallelism(1);


        TextOutputFormat<String> tof = new TextOutputFormat<>(new Path("file:///tmp/userranking"));
        tof.setWriteMode(FileSystem.WriteMode.OVERWRITE);

        userRanking.writeAsText("file:///tmp/userranking", FileSystem.WriteMode.OVERWRITE).setParallelism(1);



        env.execute("AreaCalculationProcess");

    }
    /**
     * entry point
     *
     * @param args only one argument to pass; path to config file
     */
    public static void main(String[] args) throws Exception {

        Calculator calc = new Calculator();
        calc.run(args[0]);

    }
}

package org.iidp.ostmap.batch_processing.pathcalc;


import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.iidp.ostmap.commons.accumulo.FlinkEnvManager;
import org.iidp.ostmap.commons.enums.TableIdentifier;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * This class determines the user with the longest path defined by his tweets
 */
public class PathCalculator {
    public static final String PROPERTY_INSTANCE = "accumulo.instance";
    private String accumuloInstanceName;
    public static final String PROPERTY_USER = "accumulo.user";
    private String accumuloUser;
    public static final String PROPERTY_PASSWORD = "accumulo.password";
    private String accumuloPassword;
    public static final String PROPERTY_ZOOKEEPER = "accumulo.zookeeper";
    private String accumuloZookeeper;
    public static final String inTable = TableIdentifier.RAW_TWITTER_DATA.get();
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
        job = Job.getInstance(new Configuration(), "pathCalculationJob");
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

        FlinkEnvManager fem = new FlinkEnvManager(path, "pathJob",
                TableIdentifier.RAW_TWITTER_DATA.get(),
                "HighScore");


        DataSet<Tuple2<Key,Value>> rawTwitterDataRows = fem.getDataFromAccumulo();

        DataSet<Tuple2<String,String>> geoList = rawTwitterDataRows.flatMap(new PathGeoExtrationFlatMap());

        DataSet<Tuple2<String,String>> reducedGroup = geoList
                                                        .groupBy(0)
                                                        .reduceGroup(new PathCoordGroupReduce());

        DataSet<Tuple3<String,Double,Integer>> userRanking = reducedGroup.flatMap(new PathGeoCalcFlatMap())
                .sortPartition(1, Order.DESCENDING).setParallelism(1);

        DataSet<Tuple2<Text,Mutation>> topTen = userRanking
                                                        .groupBy(2)
                                                        .reduceGroup(new TopTenGroupReduce());

        topTen.output(fem.getHadoopOF());

        fem.getExecutionEnvironment().execute("PathProcess");

        TextOutputFormat<String> tof = new TextOutputFormat<>(new Path("file:///tmp/pathuserranking"));
        tof.setWriteMode(FileSystem.WriteMode.OVERWRITE);

        userRanking.writeAsText("file:///tmp/pathuserranking", FileSystem.WriteMode.OVERWRITE).setParallelism(1);



        fem.getExecutionEnvironment().execute("PathCalculationProcess");

    }
    /**
     * entry point
     *
     * @param args only one argument to pass; path to config file
     */
    public static void main(String[] args) throws Exception {

        PathCalculator calc = new PathCalculator();
        calc.run(args[0]);

    }
}

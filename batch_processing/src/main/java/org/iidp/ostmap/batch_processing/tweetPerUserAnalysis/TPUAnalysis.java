package org.iidp.ostmap.batch_processing.tweetPerUserAnalysis;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * writes csv containg a count of tweets per user in a given timeframe
 *
 */
public class TPUAnalysis {

    public static final String PROPERTY_INSTANCE = "accumulo.instance";
    private String accumuloInstanceName;
    public static final String PROPERTY_USER = "accumulo.user";
    private String accumuloUser;
    public static final String PROPERTY_PASSWORD = "accumulo.password";
    private String accumuloPassword;
    public static final String PROPERTY_ZOOKEEPER = "accumulo.zookeeper";
    private String accumuloZookeeper;
    public static final String inTable = "RawTwitterData";
    private Job job;


    /**
     * run conversion process
     * @param configPath path to config file
     * @throws Exception
     */
    public void run(String configPath, String outputPath, long timeMin, long timeMax) throws Exception {

        readConfig(configPath);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Key,Value>> rawTwitterDataRows = getDataFromAccumulo(env);

        DataSet<Tuple2<String,Integer>> tweetCount = rawTwitterDataRows
                .filter(new TimeFilter(timeMin, timeMax))
                .map(new UserTweetMap())
                .groupBy(0)
                .sum(1);

        //tweetCount.writeAsCsv(outputPath).setParallelism(1);
        tweetCount.writeAsCsv(outputPath);


        env.execute("TPUAnalysis");

    }


    /**
     * parses the config file at the given position for the necessary parameters
     *
     * @param path
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
     * makes accumulo input accessible by flink DataSet api
     * @param env
     * @return
     * @throws IOException
     * @throws AccumuloSecurityException
     */
    private DataSet<Tuple2<Key,Value>> getDataFromAccumulo(ExecutionEnvironment env) throws IOException, AccumuloSecurityException {
        job = Job.getInstance(new Configuration(), "TPUAnalysisJob");
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
     * entry point
     *
     * @param args 1st parameter is used as path to config file
     */
    public static void main(String[] args) throws Exception {

        TPUAnalysis d = new TPUAnalysis();
        d.run(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]));

    }

}
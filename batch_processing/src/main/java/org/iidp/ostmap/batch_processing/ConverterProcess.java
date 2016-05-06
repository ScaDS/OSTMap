package org.iidp.ostmap.batch_processing;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.iidp.ostmap.commons.tokenizer.Tokenizer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * converts data from RawTwitterTable to new format and saves it to TermIndex
 *
 */
public class ConverterProcess {

    public static final String PROPERTY_INSTANCE = "accumulo.instance";
    private String accumuloInstanceName;
    public static final String PROPERTY_USER = "accumulo.user";
    private String accumuloUser;
    public static final String PROPERTY_PASSWORD = "accumulo.password";
    private String accumuloPassword;
    public static final String PROPERTY_ZOOKEEPER = "accumulo.zookeeper";
    private String accumuloZookeeper;
    public static final String inTable = "RawTwitterData";
    public static final String outTable = "TermIndex";
    private Job job;


    /**
     * run conversion process
     * @param path path to config file
     * @throws Exception
     */
    public void run(String path) throws Exception {

        readConfig(path);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Key,Value>> rawTwitterDataRows = getDataFromAccumulo(env);

        DataSet<Tuple2<Text, Mutation>> termIndexMutations = rawTwitterDataRows
                .flatMap(new ConverterFlatMap(new Tokenizer(),outTable));

        HadoopOutputFormat hof = getHadoopOF();
        termIndexMutations.output(hof);

        env.execute("ConverterProcess");

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
        job = Job.getInstance(new Configuration(), "converterJob");
        AccumuloInputFormat.setConnectorInfo(job, accumuloUser, new PasswordToken(accumuloPassword));
        AccumuloInputFormat.setScanAuthorizations(job, new Authorizations("a"));
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.withInstance(accumuloInstanceName);
        clientConfig.withZkHosts(accumuloZookeeper);
        AccumuloInputFormat.setZooKeeperInstance(job, clientConfig);
        AccumuloInputFormat.setInputTableName(job, inTable);
        return env.createHadoopInput(new AccumuloInputFormat(),Key.class,Value.class, job);
    }

    /**
     * creates output format to write data from flink DataSet to accumulo
     * @return
     * @throws AccumuloSecurityException
     */
    private HadoopOutputFormat getHadoopOF() throws AccumuloSecurityException {

        AccumuloOutputFormat.setConnectorInfo(job, accumuloUser, new PasswordToken(accumuloPassword));
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.withInstance(accumuloInstanceName);
        clientConfig.withZkHosts(accumuloZookeeper);
        AccumuloOutputFormat.setZooKeeperInstance(job, clientConfig);
        AccumuloOutputFormat.setDefaultTableName(job, outTable);
        AccumuloFileOutputFormat.setOutputPath(job,new Path("/tmp"));

        HadoopOutputFormat<Text, Mutation> hadoopOF =
                new HadoopOutputFormat<>(new AccumuloOutputFormat() , job);
        return hadoopOF;
    }


    /**
     * entry point
     *
     * @param args 1st parameter is used as path to config file
     */
    public static void main(String[] args) throws Exception {

        ConverterProcess d = new ConverterProcess();
        d.run(args[0]);

    }

}
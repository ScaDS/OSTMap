package org.iidp.ostmap.batch_processing;

import org.apache.accumulo.core.client.*;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.iidp.ostmap.commons.Tokenizer;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Martin Grimmer (martin.grimmer@mgm-tp.com)
 */
public class Driver {

    public static final String PROPERTY_INSTANCE = "accumulo.instance";
    private String accumuloInstanceName;
    public static final String PROPERTY_USER = "accumulo.user";
    private String accumuloUser;
    public static final String PROPERTY_PASSWORD = "accumulo.password";
    private String accumuloPassword;
    public static final String PROPERTY_ZOOKEEPER = "accumulo.zookeeper";
    private String accumuloZookeeper;
    private String inTable = "RawTwitterData";
    private String outTable = "TermIndex";
    private Job job;


    public void run(String path) throws Exception {

        readConfig(path);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSet<Tuple2<Key,Value>> rawTwitterDataRows = getDataFromAccumulo(env);



        DataSet<Tuple2<Key,Value>> termIndexRows = rawTwitterDataRows
                .flatMap(new Converter(new Tokenizer()));

        HadoopOutputFormat hof = getHadoopOF();
        termIndexRows.output(hof);

        env.execute("TermIndexConverter");
/*
        TextOutputFormat<String> tof = new TextOutputFormat<>(new Path("file:///tmp/wcresult"));
        tof.setWriteMode(FileSystem.WriteMode.OVERWRITE);
        wordCounts.writeAsText("file:///tmp/wcresult", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("Wordcount");
*/
    }


    /**
     * parses the config file at the given position for the necessary parameter
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
     * builds a accumulo connector
     *
     * @return the ready to use connector
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     */
    private Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        // build the accumulo connector
        Instance inst = new ZooKeeperInstance(accumuloInstanceName, accumuloZookeeper);
        Connector conn = inst.getConnector(accumuloUser, new PasswordToken(accumuloPassword));
        Authorizations auths = new Authorizations("standard");
        conn.securityOperations().changeUserAuthorizations("root", auths);
        return conn;
    }

    private DataSet<Tuple2<Key,Value>> getDataFromAccumulo(ExecutionEnvironment env) throws IOException, AccumuloSecurityException {
        job = Job.getInstance(new Configuration(), "converterJob");
        AccumuloInputFormat.setConnectorInfo(job, accumuloUser, new PasswordToken(accumuloPassword.getBytes()));
        AccumuloInputFormat.setScanAuthorizations(job, new Authorizations("standard"));
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.withInstance(accumuloInstanceName);
        clientConfig.withZkHosts(accumuloZookeeper);
        AccumuloInputFormat.setZooKeeperInstance(job, clientConfig);
        AccumuloInputFormat.setInputTableName(job, inTable);
        return env.createHadoopInput(new AccumuloInputFormat(),Key.class,Value.class, job);
    }

    private HadoopOutputFormat getHadoopOF() throws AccumuloSecurityException {

        AccumuloOutputFormat aof = new AccumuloOutputFormat();
        aof.setConnectorInfo(job, accumuloUser, new PasswordToken(accumuloPassword.getBytes()));
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.withInstance(accumuloInstanceName);
        clientConfig.withZkHosts(accumuloZookeeper);
        aof.setZooKeeperInstance(job, clientConfig);
        aof.setDefaultTableName(job, outTable);
        // Set up the Hadoop TextOutputFormat.
        HadoopOutputFormat<Text, Mutation> hadoopOF =
                new HadoopOutputFormat<Text, Mutation>(aof , job);
        return hadoopOF;
    }


    /**
     * entry point
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {

        Driver d = new Driver();
        d.run(args[0]);

    }

}
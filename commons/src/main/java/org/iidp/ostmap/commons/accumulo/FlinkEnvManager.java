package org.iidp.ostmap.commons.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
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
import org.iidp.ostmap.commons.enums.AccumuloIdentifiers;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * connects to accumulo and provides access to flink ExecutionEnvironment
 * (f.eg. for DataSet API)
 */
public class FlinkEnvManager {

    private String accumuloInstanceName;
    private String accumuloUser;
    private String accumuloPassword;
    private String accumuloZookeeper;
    private String inTable;
    private String outTable;
    private String jobName;
    private Job job;
    private ExecutionEnvironment env;


    /**
     *
     * @param configPath path for config file
     * @param jobName name used in getDataFromAccumulo
     * @param inTable input tabel name used in getDataFromAccumulo
     * @param outTable output table name used in getHadoopOF()
     * @throws IOException
     */
    public FlinkEnvManager(String configPath, String jobName,
                           String inTable, String outTable) throws IOException {
        this.inTable = inTable;
        this.outTable = outTable;
        this.jobName = jobName;
        readConfig(configPath);
    }

    /**
     * only configure accumulo (don't use flink functions)
     * @param configPath
     * @throws IOException
     */
    public FlinkEnvManager(String configPath) throws IOException {
        readConfig(configPath);
    }



    /**
     *
     * @return create new ExecutionEnvironment if null
     */
    public ExecutionEnvironment getExecutionEnvironment(){
        if(env == null){

            env = ExecutionEnvironment.getExecutionEnvironment();
        }
        return env;
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
        accumuloInstanceName = props.getProperty(AccumuloIdentifiers.PROPERTY_INSTANCE.toString());
        accumuloUser = props.getProperty(AccumuloIdentifiers.PROPERTY_USER.toString());
        accumuloPassword = props.getProperty(AccumuloIdentifiers.PROPERTY_PASSWORD.toString());
        accumuloZookeeper = props.getProperty(AccumuloIdentifiers.PROPERTY_ZOOKEEPER.toString());
    }

    /**
     *
     * @return makes accumulo input accessible by flink DataSet api, uses own env.
     * @throws IOException
     * @throws AccumuloSecurityException
     */
    public DataSet<Tuple2<Key,Value>> getDataFromAccumulo() throws IOException, AccumuloSecurityException {
        if(env == null){
            getExecutionEnvironment();
        }
        return getDataFromAccumulo(env);
    }

    /**
     * builds a accumulo connector
     *
     * @return the ready to use connector
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     */
    public Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        // build the accumulo connector
        Instance inst = new ZooKeeperInstance(accumuloInstanceName, accumuloZookeeper);
        Connector conn = inst.getConnector(accumuloUser, new PasswordToken(accumuloPassword));
        Authorizations auths = new Authorizations(AccumuloIdentifiers.AUTHORIZATION.toString());
        return conn;
    }

    /**
     * makes accumulo input accessible by flink DataSet api
     * @param env
     * @return
     * @throws IOException
     * @throws AccumuloSecurityException
     */
    public DataSet<Tuple2<Key,Value>> getDataFromAccumulo(ExecutionEnvironment env) throws IOException, AccumuloSecurityException {
        job = Job.getInstance(new Configuration(), jobName);
        AccumuloInputFormat.setConnectorInfo(job, accumuloUser, new PasswordToken(accumuloPassword));
        AccumuloInputFormat.setScanAuthorizations(job, new Authorizations(AccumuloIdentifiers.AUTHORIZATION.toString()));
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
    public HadoopOutputFormat getHadoopOF() throws AccumuloSecurityException, IOException {

        if(job == null){
            job = Job.getInstance(new Configuration(), jobName);
        }
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

}

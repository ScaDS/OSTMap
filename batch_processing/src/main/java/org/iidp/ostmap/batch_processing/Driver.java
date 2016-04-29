package org.iidp.ostmap.batch_processing;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.codehaus.jettison.json.JSONObject;
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
    private String table = "RawTwitterData";


    public void run(String path) throws Exception {

        readConfig(path);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Key,Value>> rawTwitterDataRows = getDataFromAccumulo(env);



        DataSet<Tuple2<Key,Value>> termIndexRows = rawTwitterDataRows
                .flatMap(new Converter(new Tokenizer()));


        //TODO: export

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
        Job job = Job.getInstance(new Configuration(), "getDataSet");
        AccumuloInputFormat.setConnectorInfo(job, accumuloUser, new PasswordToken(accumuloPassword.getBytes()));
        AccumuloInputFormat.setScanAuthorizations(job, new Authorizations("standard"));
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.withInstance(accumuloInstanceName);
        clientConfig.withZkHosts(accumuloZookeeper);
        AccumuloInputFormat.setZooKeeperInstance(job, clientConfig);
        AccumuloInputFormat.setInputTableName(job, table);
        return env.createHadoopInput(new AccumuloInputFormat(),Key.class,Value.class,job);
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
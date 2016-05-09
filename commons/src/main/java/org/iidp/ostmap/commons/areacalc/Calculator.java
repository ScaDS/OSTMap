package org.iidp.ostmap.commons.areacalc;


import org.apache.hadoop.mapreduce.Job;
import org.iidp.ostmap.commons.enums.TableIdentifiers;

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
    public static final String inTable = TableIdentifiers.RAW_TWITTER_TABLE.toString();
    private Job job;




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



}

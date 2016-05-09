package org.iidp.ostmap.stream_processing.types;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

/**
 * contains the connection-information and provides method to read configuration from file
 */

public class SinkConfiguration implements Serializable {

    public static final String PROPERTY_INSTANCE = "accumulo.instance";
    public String accumuloInstanceName;
    public static final String PROPERTY_USER = "accumulo.user";
    public String accumuloUser;
    public static final String PROPERTY_PASSWORD = "accumulo.password";
    public String accumuloPassword;
    public static final String PROPERTY_ZOOKEEPER = "accumulo.zookeeper";
    public String accumuloZookeeper;


    /**
     * parses the config file and takes necessary parameter
     * file must contain: accumulo.instance, accumulo.user, accumulo.password, accumulo.zookeeper.
     *
     * @param path to the config file
     * @throws IOException
     */
    public static SinkConfiguration createConfigFromFile(String path) throws IOException {
        SinkConfiguration sc = new SinkConfiguration();
        Properties props = new Properties();
        FileInputStream fis = new FileInputStream(path);
        props.load(fis);
        sc.accumuloInstanceName = props.getProperty(PROPERTY_INSTANCE);
        sc.accumuloUser = props.getProperty(PROPERTY_USER);
        sc.accumuloPassword = props.getProperty(PROPERTY_PASSWORD);
        sc.accumuloZookeeper = props.getProperty(PROPERTY_ZOOKEEPER);
        return sc;
    }

    /**
     * creates configuration with default user and password for minicluser
     *
     * @param accumuloInstanceName  the used instance's name
     * @param accumuloZookeeper     the zookeeper
     */
    public static SinkConfiguration createConfigForMinicluster(String accumuloInstanceName, String accumuloZookeeper){
        SinkConfiguration sc = new SinkConfiguration();
        sc.accumuloInstanceName = accumuloInstanceName;
        sc.accumuloZookeeper = accumuloZookeeper;
        sc.accumuloUser = "root";
        sc.accumuloPassword = "password";
        return sc;
    }

}

package org.iidp.ostmap.rest_service;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.user.GrepIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

class AccumuloService {
    private static final String PROPERTY_INSTANCE = "accumulo.instance";
    private String accumuloInstanceName;
    private static final String PROPERTY_USER = "accumulo.user";
    private String accumuloUser;
    private static final String PROPERTY_PASSWORD = "accumulo.password";
    private String accumuloPassword;
    private static final String PROPERTY_ZOOKEEPER = "accumulo.zookeeper";
    private String accumuloZookeeper;
    private static final byte[] RAW_DATA_CF = "t".getBytes();
    private static final String rawTwitterDataTableName = "RawTwitterData";
    private static final String termIndexTableName = "TermIndex";

    /**
     * Parses the config file at the given path for the necessary parameter.
     *
     * @param path the path to the config file
     * @throws IOException
     */
    void readConfig(String path) throws IOException {
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

    /**
     * Creates a scanner for the accumulo term index table.
     * @param token the token to search for
     * @param field the field to search for
     * @return a scanner instance
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    Scanner getTermIdexScanner(String token, String field) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Connector conn = getConnector();
        Authorizations auths = new Authorizations("standard");
        Scanner scan = conn.createScanner(termIndexTableName, auths);
        scan.fetchColumnFamily(new Text(field.getBytes()));
        scan.setRange(new Range(token));
        IteratorSetting grepIterSetting = new IteratorSetting(5, "grepIter", GrepIterator.class);
        GrepIterator.setTerm(grepIterSetting, token);
        scan.addScanIterator(grepIterSetting);
        return scan;
    }

    /**
     * Creates a scanner for the accumulo term index table.
     *
     * @param row the key to search for (12 byte: 8 byte timestamp (seconds since 1970) and 4 byte murmur2_32 hash of tweet)
     * @return a scanner instance
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    Scanner getRawDataScannerByRow(String row) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Connector conn = getConnector();
        Authorizations auths = new Authorizations("standard");
        Scanner scan = conn.createScanner(rawTwitterDataTableName, auths);
        scan.fetchColumnFamily(new Text(RAW_DATA_CF));
        scan.setRange(new Range(row));
        IteratorSetting grepIterSetting = new IteratorSetting(5, "grepIter", GrepIterator.class);
        GrepIterator.setTerm(grepIterSetting, row);
        scan.addScanIterator(grepIterSetting);
        return scan;
    }

    Scanner getRawDataScannerByRange(String startRowPrefix, String endRowPrefix) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Connector conn = getConnector();
        Authorizations auths = new Authorizations("standard");
        Scanner scan = conn.createScanner(rawTwitterDataTableName, auths);
        scan.fetchColumnFamily(new Text(RAW_DATA_CF));
        scan.setRange(new Range(startRowPrefix,endRowPrefix));
        //IteratorSetting grepIterSetting = new IteratorSetting(5, "grepIter", GrepIterator.class);
        //GrepIterator.setTerm(grepIterSetting, row);
        //scan.addScanIterator(grepIterSetting);
        return scan;
    }
}

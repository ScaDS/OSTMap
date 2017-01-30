package org.iidp.ostmap.commons.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.iidp.ostmap.accumuloiterators.ExtractIterator;
import org.iidp.ostmap.accumuloiterators.GeosearchExtractIterator;
import org.iidp.ostmap.commons.enums.AccumuloIdentifiers;
import org.iidp.ostmap.commons.enums.TableIdentifier;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AccumuloService {
    private String accumuloInstanceName;
    private String accumuloUser;
    private String accumuloPassword;
    private String accumuloZookeeper;



    // defines the number of threads a BatchScanner may use
    private int numberOfThreadsForScan = 16;

    /**
     * Parses the config file at the given path for the necessary parameter.
     *
     * @param path the path to the config file
     * @throws IOException
     */
    public void readConfig(String path) throws IOException {
        if (null == path) {
            throw new RuntimeException("No path to accumulo config file given. You have to start the webservice with the path to accumulo config as first parameter.");
        }
        Properties props = new Properties();
        FileInputStream fis = new FileInputStream(path);
        props.load(fis);
        accumuloInstanceName = props.getProperty(AccumuloIdentifiers.PROPERTY_INSTANCE.toString());
        accumuloUser = props.getProperty(AccumuloIdentifiers.PROPERTY_USER.toString());
        accumuloPassword = props.getProperty(AccumuloIdentifiers.PROPERTY_PASSWORD.toString());
        accumuloZookeeper = props.getProperty(AccumuloIdentifiers.PROPERTY_ZOOKEEPER.toString());
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
     * Creates a scanner for the accumulo term index table.
     *
     * @param token the token to search for
     * @param field the field to search for
     * @return a scanner instance
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    public Scanner getTermIndexScanner(String token, String field) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Connector conn = getConnector();
        Authorizations auths = new Authorizations(AccumuloIdentifiers.AUTHORIZATION.toString());
        Scanner scan = conn.createScanner(TableIdentifier.TERM_INDEX.get(), auths);
        scan.fetchColumnFamily(new Text(field.getBytes()));
        //Check if the token has a wildcard as last character
        if (hasWildCard(token)) {
            token = token.replace("*", "");
            scan.setRange(Range.prefix(token));
        } else {
            scan.setRange(Range.exact(token));
        }
        return scan;
    }

    /**
     * Builds a Range from the given start and end timestamp and returns a batch scanner.
     *
     * @param startTime start time as string
     * @param endTime   endt time as string
     * @return the batch scanner
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    public BatchScanner getRawDataScannerByTimeSpan(String startTime, String endTime) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Connector conn = getConnector();
        Authorizations auths = new Authorizations(AccumuloIdentifiers.AUTHORIZATION.toString());
        BatchScanner scan = conn.createBatchScanner(TableIdentifier.RAW_TWITTER_DATA.get(), auths, numberOfThreadsForScan);
        addReduceIterator(scan);

        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
        bb.putLong(Long.parseLong(startTime));

        ByteBuffer bb2 = ByteBuffer.allocate(Long.BYTES); //TODO: make end inclusive again
        bb2.putLong(Long.parseLong(endTime));

        List<Range> rangeList = new ArrayList<>();
        Range r = new Range(new Text(bb.array()), new Text(bb2.array()));
        rangeList.add(r);
        scan.setRanges(rangeList);
        return scan;
    }

    /**
     * Builds a batch scanner for table "RawTwitterData" by the given List of Ranges.
     *
     * @param rangeFilter the list of ranges, applied to the Batch Scanner
     * @return the batch scanner
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    public BatchScanner getRawDataBatchScanner(List<Range> rangeFilter) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Connector conn = getConnector();
        Authorizations auths = new Authorizations(AccumuloIdentifiers.AUTHORIZATION.toString());
        BatchScanner scan = conn.createBatchScanner(TableIdentifier.RAW_TWITTER_DATA.get(), auths, numberOfThreadsForScan);
        addReduceIterator(scan);
        scan.setRanges(rangeFilter);
        return scan;
    }

    /**
     * Builds a batch scanner for table "RawTwitterData" by the given List of Ranges.
     * Applies the GeosearchExtractIterator
     *
     * @param rangeFilter the list of ranges, applied to the Batch Scanner
     * @return the batch scanner
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    public BatchScanner getRawDataBatchScannerForMapView(List<Range> rangeFilter) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Connector conn = getConnector();
        Authorizations auths = new Authorizations(AccumuloIdentifiers.AUTHORIZATION.toString());
        BatchScanner scan = conn.createBatchScanner(TableIdentifier.RAW_TWITTER_DATA.get(), auths, numberOfThreadsForScan);
        addGeoReduceIterator(scan);
        scan.setRanges(rangeFilter);
        return scan;
    }

    /**
     * Creates a scanner for the accumulo tweet frequency table.
     *
     * @param startTime the start time
     * @param endTime   the end time
     * @param language may be null
     * @return a scanner instance
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    public Scanner getTweetFrequencyScanner(String startTime, String endTime, String language) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Connector conn = getConnector();
        Authorizations auths = new Authorizations(AccumuloIdentifiers.AUTHORIZATION.toString());
        Scanner scan = conn.createScanner(TableIdentifier.TWEET_FREQUENCY.get(), auths);
        scan.setRange(new Range(startTime, true, endTime, true));
        if(language != null) {
            scan.fetchColumnFamily(new Text(language));
        }
        return scan;
    }

    /**
     * Checks if the given string ends with a wildcard *
     *
     * @param token the string to check
     * @return true if ends with wildcard, false if not
     */
    private boolean hasWildCard(String token) {
        return token.endsWith("*");
    }

    /**
     * this method adds the reduce iterator to the scanner (removes unused parts from the json)
     *
     * @param scan the BatchScanner to modify
     */
    private void addReduceIterator(BatchScanner scan) {
        IteratorSetting jsonExtractIteratorConfig = new IteratorSetting(20, "jsonExtractIterator", ExtractIterator.class);
        scan.addScanIterator(jsonExtractIteratorConfig);
    }

    /**
     * this method adds the geo reduce iterator to the scanner (removes unused parts from the json)
     *
     * @param scan the BatchScanner to modify
     */
    private void addGeoReduceIterator(BatchScanner scan) {
        IteratorSetting jsonExtractIteratorConfig = new IteratorSetting(20, "jsonGeoExtractIterator", GeosearchExtractIterator.class);
        scan.addScanIterator(jsonExtractIteratorConfig);
    }

}

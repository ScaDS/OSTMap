package org.iidp.ostmap.commons.accumulo.geoTemp;

import com.github.davidmoten.geo.Coverage;
import com.github.davidmoten.geo.GeoHash;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.Text;
import org.iidp.ostmap.commons.enums.AccumuloIdentifiers;
import org.iidp.ostmap.commons.enums.TableIdentifier;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * provides easy search access for GeoTemporalIndex table
 */
public class GeoTemporalTweetQuery {

    private String accumuloInstanceName;
    private String accumuloUser;
    private String accumuloPassword;
    private String accumuloZookeeper;

    private Double north, east, south, west;
    private Long startTime, endTime;
    private Short startDay, endDay;
    private TweetCallback tc;

    GeoTemporalTweetQuery(String configPath) throws IOException {
        readConfig(configPath);

    }

    /**
     * Parses the config file at the given path for the necessary parameter.
     *
     * @param path the path to the config file
     * @throws IOException
     */
    private void readConfig(String path) throws IOException {
        if(null == path){
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
    Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        // build the accumulo connector
        Instance inst = new ZooKeeperInstance(accumuloInstanceName, accumuloZookeeper);
        Connector conn = inst.getConnector(accumuloUser, new PasswordToken(accumuloPassword));
        Authorizations auths = new Authorizations("standard");
        conn.securityOperations().changeUserAuthorizations("root", auths);
        return conn;
    }

    public void setBoundingBox(double north,
                               double east,
                               double south,
                               double west){

        this.north = north;
        this.east = east;
        this.west = west;
        this.south = south;
    }

    public void setTimeRange(long startTime, long endTime){

        this.startTime = startTime;
        this.endTime = endTime;

        LocalDate epoch = LocalDate.ofEpochDay(0);

        LocalDate startDate = (new Date(startTime)).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        startDay = (short) ChronoUnit.DAYS.between(epoch, startDate);
        LocalDate endDate = (new Date(startTime)).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        endDay = (short) ChronoUnit.DAYS.between(epoch, endDate);
    }

    public void setCallback(TweetCallback tc){

        this.tc = tc;
    }

    public void query() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        if(north == null ||
                east == null ||
                west == null ||
                south == null ||
                startTime == null ||
                endTime == null ||
                tc == null){

            System.err.println("Error: not all query parameters set");
            return;
        }


        Connector conn = getConnector();
        Authorizations auths = new Authorizations("standard");
        BatchScanner scan = conn.createBatchScanner(TableIdentifier.GEO_TEMPORAL_INDEX.get(), auths,32);

        List<Range> rangeList = getRangeList();

        scan.setRanges(rangeList);
        for (Map.Entry<Key, Value> entry : scan) {

            //TODO: filter exact window
            if(true){

                //TODO: get RawTwiiterData value
                tc.process(entry.getValue().toString());
            }
        }

        scan.close();
    }


    /**
     *
     * @return ranges for rowkeys [0-255][startDay-endDay][setOfGeohashes]
     */
    private List<Range> getRangeList(){
        List<Range> rangeList = new ArrayList<>();

        Coverage coverage = GeoHash.coverBoundingBox(west,north,east,south,8);


        List<Tuple2<String,String>> hashRanges = mergeHashes(coverage.getHashes());

        //TODO: gereate Ranges
        return rangeList;
    }

    /**
     * creates a list of ranges (Tuple2<startHash, endHash>) from a set of hashes
     * @param hashStrings
     * @return
     */
    private List<Tuple2<String,String>> mergeHashes(Set<String> hashStrings){

        List<Tuple2<String,String>> mergedHashes = new ArrayList<>();

        List<String> sortedHashes = new ArrayList<>(hashStrings);
        Collections.sort(sortedHashes);

        int i = 0;
        while(i < sortedHashes.size()){

            Tuple2<String,String > nextTupel = new Tuple2<>();
            nextTupel.f0 = sortedHashes.get(i);
            //Todo:this
        }

        return mergedHashes;
    }

    private Boolean isNext(String hash1, String hash2){
        //TODO:this
        return true;
    }
}

package org.iidp.ostmap.commons.accumulo.geoTemp;

import com.github.davidmoten.geo.Coverage;
import com.github.davidmoten.geo.GeoHash;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.iidp.ostmap.accumuloiterators.GeoTempFilter;
import org.iidp.ostmap.commons.accumulo.FlinkEnvManager;
import org.iidp.ostmap.commons.enums.TableIdentifier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * provides easy search access for GeoTemporalIndex table
 */
public class GeoTemporalTweetQuery {

    private Double north, east, south, west;
    private Long startTime, endTime;
    private Short startDay, endDay;
    private TweetCallback tc;
    private FlinkEnvManager fem;

    public GeoTemporalTweetQuery(String configPath) throws IOException {

        fem = new FlinkEnvManager(configPath);

    }

    public GeoTemporalTweetQuery(){

    }

    public void setConfig(String configPath)throws IOException {

        fem = new FlinkEnvManager(configPath);

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
                tc == null ||
                fem == null){

            System.err.println("Error: not all query parameters set");
            return;
        }


        Connector conn = fem.getConnector();
        Authorizations auths = new Authorizations("standard");
        BatchScanner geoTempScan = conn.createBatchScanner(TableIdentifier.GEO_TEMPORAL_INDEX.get(), auths,32);
        BatchScanner rawTwitterScan = conn.createBatchScanner(TableIdentifier.RAW_TWITTER_DATA.get(),auths,32);


        System.out.println("getList");
        List<Range> geoRangeList = getRangeList();
        geoTempScan.setRanges(geoRangeList);

        //set filter for exact query window
        IteratorSetting filterIteratorConfig = new IteratorSetting(20, "GeoTempFilterIterator", GeoTempFilter.class);
        GeoTempFilter.setBoundingBox(filterIteratorConfig,north,east,
                south,west,startTime,endTime);

        geoTempScan.addScanIterator(filterIteratorConfig);

        System.out.println("getKeys");
        //query on GeoTempoalIndex for RawTwitterKeys
        List<Range> rawRangeList = new ArrayList<>();
        for (Map.Entry<Key, Value> entry : geoTempScan) {

            rawRangeList.add(new Range(entry.getKey().getColumnFamily()));
        }
        geoTempScan.close();

        System.out.println("getRaw");
        //query on RawTwitterData
        rawTwitterScan.setRanges(rawRangeList);
        for(Map.Entry<Key, Value> entry : rawTwitterScan){

            tc.process(entry.getValue().toString());
        }
        rawTwitterScan.close();
    }

    /**
     *
     * @return ranges for rowkeys [0-255][startDay-endDay][setOfGeohashes]
     */
    private List<Range> getRangeList(){
        List<Range> rangeList = new ArrayList<>();

        System.out.println("getCoverage");
        Coverage coverage = GeoHash.coverBoundingBox(north,west,south,east,8);

        System.out.println("getHashes");
        Set<String> hashes = coverage.getHashes();

        System.out.println("for0");
        for(String hash: hashes){
            System.out.println("for1");
            for(short day = startDay; day < endDay; day++){
                System.out.println("for2");
                for(byte spreadingByte = (byte) 0; spreadingByte < 255; spreadingByte++){
                    System.out.println("for3");

                    ByteBuffer startKey = ByteBuffer.allocate(11);
                    startKey.put(spreadingByte).putShort(day).put(hash.getBytes());

                    rangeList.add(new Range(new Text(startKey.array())));
                }
            }
        }

        return rangeList;
    }

    /**
     * creates a list of successive subsets of hashes (Tuple2<startHash, endHash>) from a set of hashes
     * @param hashStrings
     * @return
     */
   /* protected List<Tuple2<String,String>> mergeHashes(Set<String> hashStrings){

        List<Tuple2<String,String>> mergedHashes = new ArrayList<>();

        List<String> sortedHashes = new ArrayList<>(hashStrings);
        Collections.sort(sortedHashes);

        int i = 0;
        boolean isSuccessive;
        while(i < sortedHashes.size()){

            Tuple2<String,String > nextTupel = new Tuple2<>();
            nextTupel.f0 = sortedHashes.get(i);
            nextTupel.f1 = sortedHashes.get(i);
            do{
                isSuccessive = false;
                if(i+1 < sortedHashes.size() && isNext(sortedHashes.get(i),sortedHashes.get(i+1))){

                    i++;
                    nextTupel.f1 = sortedHashes.get(i);
                    isSuccessive = true;
                }
            }while(isSuccessive);

            mergedHashes.add(nextTupel);
            i++;
        }

        return mergedHashes;
    }*/

    /**
     * checks if two hashes are successive
     * @param hash1
     * @param hash2
     * @return
     */
  /*  protected Boolean isNext(String hash1, String hash2){

        return hash2.equals(getNextHash(hash1));
    }

    /**
     * calculates the next hash
     * @param hash
     * @return
     */
  /*  protected static String getNextHash(String hash) {
        long decode = Base32.decodeBase32(hash);
        decode++;
        return Base32.encodeBase32(decode,8);
    }*/
}

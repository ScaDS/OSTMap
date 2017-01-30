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
import org.iidp.ostmap.commons.accumulo.AccumuloService;
import org.iidp.ostmap.commons.enums.AccumuloIdentifiers;
import org.iidp.ostmap.commons.enums.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    static Logger log = LoggerFactory.getLogger(GeoTemporalTweetQuery.class);

    private Double north, east, south, west;
    private Long startTime, endTime;
    private Short startDay, endDay;
    private TweetCallback tc;
    private AccumuloService ac;

    public GeoTemporalTweetQuery(String configPath) throws IOException {
        ac = new AccumuloService();
        ac.readConfig(configPath);
    }

    public GeoTemporalTweetQuery() {
    }

    public void setConfig(String configPath) throws IOException {
        ac = new AccumuloService();
        ac.readConfig(configPath);
    }


    public void setBoundingBox(double north,
                               double east,
                               double south,
                               double west) {
        this.north = north;
        this.east = east;
        this.west = west;
        this.south = south;
        log.debug("bounding box: [" + north + "," + east + "," + south + "," + west + "]");
    }

    /**
     * set time range for query
     *
     * @param startTime seconds since epoch
     * @param endTime   sceconds since epoch
     */
    public void setTimeRange(long startTime, long endTime) {

        this.startTime = startTime;
        this.endTime = endTime;

        LocalDate epoch = LocalDate.ofEpochDay(0);

        //*1000 convert seconds to ms since epoch
        LocalDate startDate = (new Date(startTime * 1000L)).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        startDay = (short) ChronoUnit.DAYS.between(epoch, startDate);
        LocalDate endDate = (new Date(endTime * 1000L)).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        endDay = (short) ChronoUnit.DAYS.between(epoch, endDate);
        if (log.isDebugEnabled()) {
            log.debug("time range: ["
                            + startDate.toString()
                            + "(" + startDay + ")"
                            + " - " + endDate.toString()
                            + "(" + endDay + ")]"
            );
        }
    }

    public void setCallback(TweetCallback tc) {

        this.tc = tc;
    }

    /**
     * executes query, calls callback.process() for each result
     *
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    public void query() throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

        long startNano = System.nanoTime();
        if (north == null ||
                east == null ||
                west == null ||
                south == null ||
                startTime == null ||
                endTime == null ||
                tc == null) {

            log.error("not all query parameters set: "
                    + north + " "
                    + east + " "
                    + south + " "
                    + west + " "
                    + startTime + " "
                    + endTime + " "
                    + tc);
            return;
        }


        Authorizations auths = new Authorizations(AccumuloIdentifiers.AUTHORIZATION.toString());
        BatchScanner geoTempScan = ac.getConnector().createBatchScanner(TableIdentifier.GEO_TEMPORAL_INDEX.get(), auths, 32);


        List<Range> geoRangeList = getRangeList();
        if (geoRangeList.size() == 0) {
            log.error("range list is empty!");
            return;
        }

        // show what we will search for
        if (log.isDebugEnabled()) {

            byte[] firstRow = geoRangeList.get(0).getStartKey().getRow().copyBytes();
            byte[] lastRow = geoRangeList.get(geoRangeList.size() - 1).getStartKey().getRow().copyBytes();
            log.debug("Ranges: [" + geoRangeList.size()
                    + ", first: (" + new String(firstRow) + " == " + GeoTemporalKey.rowBytesToString(firstRow)
                    + ") to last:  (" + new String(lastRow) + " == " + GeoTemporalKey.rowBytesToString(lastRow) + ")]");
        }

        geoTempScan.setRanges(geoRangeList);

        //set filter for exact query window
        IteratorSetting filterIteratorConfig = new IteratorSetting(20, "GeoTempFilterIterator", GeoTempFilter.class);
        GeoTempFilter.setBoundingBox(filterIteratorConfig, north, east, south, west, startTime, endTime);

        log.debug("IteratorConfig: [" + filterIteratorConfig + "]");

        geoTempScan.addScanIterator(filterIteratorConfig);

        //query on GeoTempoalIndex for RawTwitterKeys
        List<Range> rawRangeList = new ArrayList<>();
        for (Map.Entry<Key, Value> entry : geoTempScan) {
            rawRangeList.add(new Range(entry.getKey().getColumnFamily()));
        }
        geoTempScan.close();


        if (log.isDebugEnabled()) {
            long stopNano = System.nanoTime();
            double diffSeconds = (stopNano - startNano) / 1000000000.0f;
            startNano = System.nanoTime();
            log.debug("seconds for geotemp index: " + diffSeconds);
            log.debug("number of lookups in " + TableIdentifier.RAW_TWITTER_DATA.get() + ": " + rawRangeList.size());
        }

        if (rawRangeList.size() == 0) {
            return;
        }
        //query on RawTwitterData
        BatchScanner rawTwitterScan = ac.getRawDataBatchScannerForMapView(rawRangeList);

        for (Map.Entry<Key, Value> entry : rawTwitterScan) {
            tc.process(entry.getValue().toString());
        }
        rawTwitterScan.close();

        if (log.isDebugEnabled()) {
            long stopNano = System.nanoTime();
            double diffSeconds = (stopNano - startNano) / 1000000000.0f;
            log.debug("seconds for result lookups: " + diffSeconds);
        }

    }

    /**
     * @return ranges for rowkeys [0-255][startDay-endDay][setOfGeohashes]
     */
    private List<Range> getRangeList() {
        List<Range> rangeList = new ArrayList<>();

        Coverage coverage = GeoHash.coverBoundingBoxMaxHashes(north, west, south, east, 100);
        log.debug("coverage:  [size:" + coverage.getHashes().size() + ", ratio:" + coverage.getRatio() + "]");

        Set<String> hashes = coverage.getHashes();
        for (String hash : hashes) {
            for (short day = startDay; day <= endDay; day++) {
                for (int spreadingByte = 0; spreadingByte <= 255; spreadingByte++) {
                    ByteBuffer startKey = ByteBuffer.allocate(3 + hash.length());
                    if (hash.length() > 8) {
                        hash = hash.substring(0, 8);
                    }
                    startKey.put((byte) spreadingByte).putShort(day).put(hash.getBytes());
                    rangeList.add(Range.prefix(new Text(startKey.array())));
                }
            }
        }

        return rangeList;
    }
}

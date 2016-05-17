package org.iidp.ostmap.commons.accumulo.geoTemp;

import com.github.davidmoten.geo.GeoHash;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.commons.codec.Charsets;
import org.codehaus.jettison.json.JSONException;
import org.iidp.ostmap.commons.extractor.Extractor;


import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

/**
 * Class containing the geoTemporal-key's row and columnQualifier
 * and static functionality for building the named values
 */
public class GeoTemporalKey {

    /** ------ row -------------- */
    //spreading byte + day + geohash = 11 bytes
    public byte[] rowBytes;

    //murmur_32 of original json string modulo 255
    protected int spreadingByte;

    //days since 1.1.1970
    protected short day;

    //8 byte long string created using com.github.davidmoten:geo:0.7.1
    protected String geoHash;

    /** ------ column qualifier -------------- */
    // lat + long as 2*4b
    public byte[] columQualifier;


    /** ------ hash and formatter -------------- */

    //the murmur-hash-function
    private static HashFunction hash = Hashing.murmur3_32();

    //the formatter for extracting date
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy").withLocale(Locale.ENGLISH);


    // empty constructor
    public GeoTemporalKey() {}

    /**
     * Builds the key's row and columnQualifier byte-arrays for a given tweet
     * @param json the tweet's json
     * @return  the GeoTemporalKey-object with null value for rowBytes/columnQualifier if error occurs
     */
    public static GeoTemporalKey buildKey(String json) {
        GeoTemporalKey key = new GeoTemporalKey();
        Double[] loc = null;

        loc = Extractor.extractLocation(json);

        if(loc!=null) {
            key.spreadingByte = calcSpreadingByte(json);
            key.day = calcDay(json);
            key.geoHash = calcGeoHash(loc);
            key.columQualifier = calcColumnQualifier(loc);
            // 1b + 2b + 8b = 11b
            ByteBuffer bb = ByteBuffer.allocate(11);
            bb.put((byte)(key.spreadingByte & 0xFF))            // first byte of int
                    .putShort(key.day)                              // day as short
                    .put(key.geoHash.getBytes(Charsets.UTF_8));           // geoHash-String's bytes
            key.rowBytes = bb.array();
        }
        else {
            key.rowBytes = null;
            key.columQualifier = null;
        }

        return key;
    }

    /**
     * Calculates spreadingByte (hash modulo 255)
     * @param json  tweet's json
     * @return      the calculated int value
     */
    private static int calcSpreadingByte(String json) {
        int bufferSize = json.length();
        ByteBuffer bb = ByteBuffer.allocate(bufferSize);
        bb.put(json.getBytes(Charsets.UTF_8));
        return hash.hashBytes(bb.array()).asInt() % 255;
    }

    /**
     * Calculates days since 1.1.1970
     * @param json  tweet's json
     * @return      number of days as short value
     */
    private static short calcDay(String json) {
        short day = 0;
        int pos1 = json.indexOf("\"created_at\":\"");
        int pos2 = pos1 + 14;
        int pos3 = json.indexOf("\",\"", pos2);
        if (pos1 != -1 && pos2 != -1) {
            String rawTime = json.substring(pos2, pos3);
            ZonedDateTime time = ZonedDateTime.parse(rawTime, formatter);
            LocalDate epoch = LocalDate.ofEpochDay(0);
            day = (short) ChronoUnit.DAYS.between(epoch, time);
        }
        return day;
    }

    /**
     * Calculates the geoHash
     * @param loc   location as long-lat-array
     * @return      the 8 character long hash
     */
    private static String calcGeoHash(Double[] loc) {

        return GeoHash.encodeHash(loc[1], loc[0], 8);
    }

    /**
     * Calculates the location-byte-array
     * @param loc   location as long-lat-array
     * @return      the location-byte-array (4byte lat + 4byte long = 8byte)
     */
    private static byte[] calcColumnQualifier(Double[] loc) {
        return ByteBuffer.allocate(8).putFloat(loc[1].floatValue()).putFloat(loc[0].floatValue()).array();
    }

}

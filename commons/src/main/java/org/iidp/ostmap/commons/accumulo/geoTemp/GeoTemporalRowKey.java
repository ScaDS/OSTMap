package org.iidp.ostmap.commons.accumulo.geoTemp;

import com.github.davidmoten.geo.GeoHash;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.commons.codec.Charsets;


import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

/**
 * Class containing the geoTemporal-key-row
 * and static functionality for building it
 */
public class GeoTemporalRowKey {

    /** ------ row -------------- */
    //spreading byte + day + geohash = 11 bytes
    public byte[] rowBytes;

    //murmur_32 of original json string modulo 255
    public int spreadingByte;

    //days since 1.1.1970
    public short day;

    //8 byte long string created using com.github.davidmoten:geo:0.7.1
    public String geoHash;

    //the murmur-hash-function
    private static HashFunction hash = Hashing.murmur3_32();

    //the formatter for extracting date
    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy").withLocale(Locale.ENGLISH);


    // empty constructor
    public GeoTemporalRowKey() {}

    /**
     * Builds the key's row byte-array for a given tweet
     * @param json the tweet's json
     * @return
     */
    public static GeoTemporalRowKey buildRowKey(String json) {
        GeoTemporalRowKey key = new GeoTemporalRowKey();
        key.spreadingByte = calcSpreadingByte(json);
        key.day = calcDay(json);
        key.geoHash = calcGeoHash(json);

        // 1b + 2b + 8b = 11b
        ByteBuffer bb = ByteBuffer.allocate(11);
        bb.put((byte)(key.spreadingByte & 0xFF))        // first byte of int
                .putShort(key.day)                      // day as short
                .put(key.geoHash.getBytes());           // geoHash-String's bytes
        key.rowBytes = bb.array();
        return key;
    }

    public static int calcSpreadingByte(String json) {
        int bufferSize = json.length();
        ByteBuffer bb = ByteBuffer.allocate(bufferSize);
        bb.put(json.getBytes(Charsets.UTF_8));
        return hash.hashBytes(bb.array()).asInt() % 255;
    }

    public static short calcDay(String json) {
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

    public static String calcGeoHash(String json) {
        Double[] loc = Extractor.extractLocation(json);
        return GeoHash.encodeHash(loc[0], loc[1], 8);
    }

}

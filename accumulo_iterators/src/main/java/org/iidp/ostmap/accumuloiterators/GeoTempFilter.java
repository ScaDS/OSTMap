package org.iidp.ostmap.accumuloiterators;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * filter for exact window of GeoTemporalTweetQuery
 */
public class GeoTempFilter extends Filter{

    private Double north,east,south,west;
    private Long startTime,endTime;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);

        this.north = Double.parseDouble(options.get("north"));
        this.east = Double.parseDouble(options.get("east"));
        this.west = Double.parseDouble(options.get("west"));
        this.south = Double.parseDouble(options.get("south"));

        this.startTime = Long.parseLong(options.get("startTime"));
        this.endTime = Long.parseLong(options.get("endTime"));

    }

    public static void setBoundingBox(IteratorSetting config, double north,
                                      double east, double south, double west,
                                      long startTime, long endTime) {

        config.addOption("north", Double.toString(north));
        config.addOption("east", Double.toString(east));
        config.addOption("west", Double.toString(west));
        config.addOption("south", Double.toString(south));

        config.addOption("startTime", Long.toString(startTime));
        config.addOption("endTime", Long.toString(endTime));

    }

    @Override
    public boolean accept(Key k, Value v) {

        Long tweetTime = ByteBuffer.wrap(k.getColumnFamily().getBytes()).getLong();

        ByteBuffer bb = ByteBuffer.wrap(k.getColumnQualifier().getBytes());

        float tweetLat = bb.getFloat();
        float tweetLon = bb.getFloat();

        return startTime < tweetTime &&
                tweetTime < endTime &&
                west < tweetLon &&
                tweetLon < east &&
                south < tweetLat &&
                tweetLat < north;


    }
}

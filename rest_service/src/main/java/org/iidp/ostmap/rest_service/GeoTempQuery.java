package org.iidp.ostmap.rest_service;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.iidp.ostmap.commons.accumulo.geoTemp.GeoTemporalTweetQuery;
import org.iidp.ostmap.commons.accumulo.geoTemp.TweetCallback;
import org.iidp.ostmap.rest_service.helper.JsonHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GeoTempQuery implements TweetCallback {

    static Logger log = LoggerFactory.getLogger(GeoTempQuery.class);

    private String northCoordinate,
            eastCoordinate,
            southCoordinate,
            westCoordinate,
            startTime,
            endTime;
    private String config;

    private StringBuilder resultBuilder;
    private boolean isFirstResult;


    public GeoTempQuery(String northCoordinate,
                        String eastCoordinate,
                        String southCoordinate,
                        String westCoordinate,
                        String startTime,
                        String endTime,
                        String configPath) {
        this.northCoordinate = northCoordinate;
        this.eastCoordinate = eastCoordinate;
        this.southCoordinate = southCoordinate;
        this.westCoordinate = westCoordinate;
        this.startTime = startTime;
        this.endTime = endTime;
        this.config = configPath;
        resultBuilder = new StringBuilder(10000);
        isFirstResult = true;

    }

    public String getResult() throws IOException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        resultBuilder.append("[");
        GeoTemporalTweetQuery query = new GeoTemporalTweetQuery(config);
        query.setBoundingBox(
                Double.parseDouble(northCoordinate),
                Double.parseDouble(eastCoordinate),
                Double.parseDouble(southCoordinate),
                Double.parseDouble(westCoordinate));
        query.setTimeRange(
                Long.parseLong(startTime),
                Long.parseLong(endTime));
        query.setCallback(this);
        // start the query -> process is called once for each result
        query.query();
        resultBuilder.append("]");
        return resultBuilder.toString();
    }

    @Override
    public void process(String json) {
        if (!isFirstResult) {
            resultBuilder.append(",");
        } else {
            isFirstResult = false;
        }
        json = JsonHelper.generateCoordinates(json);
        resultBuilder.append(json);
    }
}

package org.iidp.ostmap.rest_service;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.Objects;

@Controller
@RequestMapping("/api")
public class GeoTimePeriodController {

    static Logger log = LoggerFactory.getLogger(GeoTimePeriodController.class);

    /**
     * Mapping method for path /geotemporalsearch
     *
     * @param northCoordinate
     * @param eastCoordinate
     * @param southCoordinate
     * @param westCoordinate
     * @param startTime
     * @param endTime
     * @return a json response
     */
    @RequestMapping(
            value = "/geotemporalsearch",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE}
    )
    @ResponseBody
    String getTweetsByGeoAndTime(
            @RequestParam(name = "bbnorth") String northCoordinate,
            @RequestParam(name = "bbeast") String eastCoordinate,
            @RequestParam(name = "bbsouth") String southCoordinate,
            @RequestParam(name = "bbwest") String westCoordinate,
            @RequestParam(name = "tstart") String startTime,
            @RequestParam(name = "tend") String endTime,
            @RequestParam(name = "topten", required = false, defaultValue = "false") Boolean topten
    ) throws AccumuloException, TableNotFoundException, AccumuloSecurityException, IOException {
        log.debug("GeoTemporalQuery #################################");
        validateQueryParams(
                northCoordinate,
                eastCoordinate,
                southCoordinate,
                westCoordinate,
                startTime,
                endTime);

        // build query
        GeoTempQuery geoTempQuery = new GeoTempQuery(
                northCoordinate,
                eastCoordinate,
                southCoordinate,
                westCoordinate,
                startTime,
                endTime,
                MainController.configFilePath);

        // submit query and return result
        return geoTempQuery.getResult();

    }

    /**
     * Mapping method for path /testgeo
     *
     * @param paramNorthCoordinate
     * @param paramEastCoordinate
     * @param paramSouthCoordinate
     * @param paramWestCoordinate
     * @param paramStartTime
     * @param paramEndTime
     * @return a json response
     */
    @RequestMapping(
            value = "/testgeo",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE}
    )
    @ResponseBody
    String getTweetsByGeoAndTimeToTest(
            @RequestParam(name = "bbnorth") String paramNorthCoordinate,
            @RequestParam(name = "bbeast") String paramEastCoordinate,
            @RequestParam(name = "bbsouth") String paramSouthCoordinate,
            @RequestParam(name = "bbwest") String paramWestCoordinate,
            @RequestParam(name = "tstart") String paramStartTime,
            @RequestParam(name = "tend") String paramEndTime
    ) {
        validateQueryParams(
                paramNorthCoordinate,
                paramEastCoordinate,
                paramSouthCoordinate,
                paramWestCoordinate,
                paramStartTime,
                paramEndTime);

        return MainController.getTestTweets();
    }

    /**
     * Validates the Query parameters.
     */
    void validateQueryParams(String northCoordinate,
                             String eastCoordinate,
                             String southCoordinate,
                             String westCoordinate,
                             String startTime,
                             String endTime
    ) throws IllegalArgumentException {
        long minTimestamp = 1451606400; //unix time in sec 01.01.2016 00:00 Uhr

        if (northCoordinate == null || Objects.equals(northCoordinate, "") || northCoordinate.length() < 2 || !isFloat(northCoordinate)) {
            throw new IllegalArgumentException("Query parameter 'bborth' is null or not a float number.");
        }
        if (eastCoordinate == null || Objects.equals(eastCoordinate, "") || eastCoordinate.length() < 2 || !isFloat(eastCoordinate)) {
            throw new IllegalArgumentException("Query parameter 'bbeast' is null or not a float number.");
        }
        if (southCoordinate == null || Objects.equals(southCoordinate, "") || southCoordinate.length() < 2 || !isFloat(southCoordinate)) {
            throw new IllegalArgumentException("Query parameter 'bbsouth' is null or not a float number.");
        }
        if (westCoordinate == null || Objects.equals(westCoordinate, "") || westCoordinate.length() < 2 || !isFloat(westCoordinate)) {
            throw new IllegalArgumentException("Query parameter 'bbwest' is null or not a float number.");
        }
        long tstart = Long.parseLong(startTime);
        long tend = Long.parseLong(endTime);
        if (startTime == null || Objects.equals(startTime, "") || startTime.length() != 10 || tstart < minTimestamp) {
            throw new IllegalArgumentException("Query parameter 'tstart' is null, has more or less thant 10 digits or is smaller than " + String.valueOf(minTimestamp) + ".");
        }
        if (endTime == null || Objects.equals(endTime, "") || endTime.length() != 10 || tend < minTimestamp) {
            throw new IllegalArgumentException("Query parameter 'tend' is null, has more or less thant 10 digits or is smaller than " + String.valueOf(minTimestamp) + ".");
        }
        if (tstart > tend) {
            throw new IllegalArgumentException("Query parameter 'tstart' is bigger than 'tend.");
        }
    }

    /**
     * Checks if the given string is a float
     *
     * @param checkString the string to check
     * @return true = float, false = others
     */
    private boolean isFloat(String checkString) {
        boolean isFloat = false;
        try {
            Float.parseFloat(checkString);
            isFloat = true;
        } catch (NumberFormatException e) {
            isFloat = false;
        }
        return isFloat;
    }
}

package org.iidp.ostmap.rest_service;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

@Controller
@RequestMapping("/api")
public class GeoTimePeriodController {
    private String _paramNorthCoordinate,
            _paramEastCoordinate,
            _paramSouthCoordinate,
            _paramWestCoordinate,
            _paramStartTime,
            _paramEndTime ;

    /**
     * Mapping method for path /geotemporalsearch
     * @param paramNorthCoordinate
     * @param paramEastCoordinate
     * @param paramSouthCoordinate
     * @param paramWestCoordinate
     * @param paramStartTime
     * @param paramEndTime
     * @return a json response
     */
    @RequestMapping(
            value = "/geotemporalsearch",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE}
    )
    @ResponseBody
    String getTweetsByGeoAndTime(
            @RequestParam(name = "bbnorth") String paramNorthCoordinate,
            @RequestParam(name = "bbeast")  String paramEastCoordinate,
            @RequestParam(name = "bbsouth") String paramSouthCoordinate,
            @RequestParam(name = "bbwest")  String paramWestCoordinate,
            @RequestParam(name = "tstart")  String paramStartTime,
            @RequestParam(name = "tend")    String paramEndTime
    ) {
        _paramNorthCoordinate = paramNorthCoordinate;
        _paramEastCoordinate = paramEastCoordinate;
        _paramSouthCoordinate = paramSouthCoordinate;
        _paramWestCoordinate = paramWestCoordinate;
        _paramStartTime = paramStartTime;
        _paramEndTime = paramEndTime;

        validateQueryParams();

        return getResultsFromAccumulo(MainController.configFilePath);
    }

    /**
     * Mapping method for path /testgeo
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
            @RequestParam(name = "bbeast")  String paramEastCoordinate,
            @RequestParam(name = "bbsouth") String paramSouthCoordinate,
            @RequestParam(name = "bbwest")  String paramWestCoordinate,
            @RequestParam(name = "tstart")  String paramStartTime,
            @RequestParam(name = "tend")    String paramEndTime
    ) {
        _paramNorthCoordinate = paramNorthCoordinate;
        _paramEastCoordinate = paramEastCoordinate;
        _paramSouthCoordinate = paramSouthCoordinate;
        _paramWestCoordinate = paramWestCoordinate;
        _paramStartTime = paramStartTime;
        _paramEndTime = paramEndTime;

        validateQueryParams();

        return MainController.getTestTweets();
    }

    String getResultsFromAccumulo(String configFilePath){
        String result = "[";
        AccumuloService accumuloService = new AccumuloService();
        BatchScanner rawDataScanner;

        double north = Double.parseDouble(_paramNorthCoordinate);
        double west = Double.parseDouble(_paramWestCoordinate);
        double south = Double.parseDouble(_paramSouthCoordinate);
        double east = Double.parseDouble(_paramEastCoordinate);

        try {
            accumuloService.readConfig(configFilePath);

            rawDataScanner = accumuloService.getRawDataScannerByTimeSpan(_paramStartTime,_paramEndTime);
            boolean isFirst = true;

            for (Map.Entry<Key, Value> rawDataEntry : rawDataScanner) {
                String json = rawDataEntry.getValue().toString();

                Double[] longLat = Extractor.extractLocation(json);
                if (longLat != null && longLat[0] != null && longLat[1] != null) {
                    Double longitude = longLat[0];
                    Double latitude = longLat[1];
                    //TODO: does this work across meridians?
                    if(west < longitude && longitude < east &&
                            south < latitude && latitude < north){

                        if(!isFirst){
                            result += ",";
                        }else{

                            isFirst=false;
                        }

                        result += json;
                    }
                }
            }

            rawDataScanner.close();
        } catch (IOException | AccumuloSecurityException | AccumuloException | TableNotFoundException  e) {
            throw new RuntimeException("There was a failure during Accumulo communication.",e);
        } catch (JSONException jse){

        }
        result += "]";
        return result;
    }

    /**
     * Validates the Query parameters.
     */
    void validateQueryParams() throws IllegalArgumentException
    {
        long minTimestamp = 1451606400; //unix time in sec 01.01.2016 00:00 Uhr

        if(_paramNorthCoordinate == null || Objects.equals(_paramNorthCoordinate, "") || _paramNorthCoordinate.length() < 2 || !isFloat(_paramNorthCoordinate)){
            throw new IllegalArgumentException("Query parameter 'bborth' is null or not a float number.");
        }
        if(_paramEastCoordinate == null || Objects.equals(_paramEastCoordinate, "") || _paramEastCoordinate.length() < 2 || !isFloat(_paramEastCoordinate)){
            throw new IllegalArgumentException("Query parameter 'bbeast' is null or not a float number.");
        }
        if(_paramSouthCoordinate == null || Objects.equals(_paramSouthCoordinate, "") || _paramSouthCoordinate.length() < 2 || !isFloat(_paramSouthCoordinate)){
            throw new IllegalArgumentException("Query parameter 'bbsouth' is null or not a float number.");
        }
        if(_paramWestCoordinate == null || Objects.equals(_paramWestCoordinate, "") || _paramWestCoordinate.length() < 2 || !isFloat(_paramWestCoordinate)){
            throw new IllegalArgumentException("Query parameter 'bbwest' is null or not a float number.");
        }
        long tstart = Long.parseLong(_paramStartTime);
        long tend = Long.parseLong(_paramEndTime);
        if(_paramStartTime == null || Objects.equals(_paramStartTime, "") || _paramStartTime.length() != 10 || tstart < minTimestamp){
            throw new IllegalArgumentException("Query parameter 'tstart' is null, has more or less thant 10 digits or is smaller than " + String.valueOf(minTimestamp) + ".");
        }
        if(_paramEndTime == null || Objects.equals(_paramEndTime, "") || _paramEndTime.length() != 10 || tend < minTimestamp){
            throw new IllegalArgumentException("Query parameter 'tend' is null, has more or less thant 10 digits or is smaller than " + String.valueOf(minTimestamp) + ".");
        }
        if(tstart > tend){
            throw new IllegalArgumentException("Query parameter 'tstart' is bigger than 'tend.");
        }
    }

    /**
     * Checks if the given string is a float
     * @param checkString the string to check
     * @return true = float, false = others
     */
    private boolean isFloat(String checkString){
        boolean isFloat = false;
        try {
            Float.parseFloat(checkString);
            isFloat = true;
        } catch (NumberFormatException e){
            isFloat = false;
        }
        return isFloat;
    }

    public void set_paramNorthCoordinate(String _paramNorthCoordinate) {
        this._paramNorthCoordinate = _paramNorthCoordinate;
    }

    public void set_paramEastCoordinate(String _paramEastCoordinate) {
        this._paramEastCoordinate = _paramEastCoordinate;
    }

    public void set_paramSouthCoordinate(String _paramSouthCoordinate) {
        this._paramSouthCoordinate = _paramSouthCoordinate;
    }

    public void set_paramWestCoordinate(String _paramWestCoordinate) {
        this._paramWestCoordinate = _paramWestCoordinate;
    }

    public void set_paramStartTime(String _paramStartTime) {
        this._paramStartTime = _paramStartTime;
    }

    public void set_paramEndTime(String _paramEndTime) {
        this._paramEndTime = _paramEndTime;
    }
}

package org.iidp.ostmap.rest_service;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.codehaus.jettison.json.JSONObject;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.ByteBuffer;
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

        String resultList = "";

        if(validateQueryParams())
        {
            resultList = getResultsFromAccumulo();
        }else{
            throw new IllegalArgumentException();
        }

        return resultList;
    }

    public String getResultsFromAccumulo(){
        String result = "";
        AccumuloService accumuloService = new AccumuloService();

        try {
            accumuloService.readConfig(MainController.configFilePath);

            result = getResult(accumuloService,_paramStartTime,_paramEndTime,
                    Double.parseDouble(_paramNorthCoordinate),
                    Double.parseDouble(_paramEastCoordinate),
                    Double.parseDouble(_paramSouthCoordinate),
                    Double.parseDouble(_paramWestCoordinate));

        } catch (IOException ioe){
            ioe.printStackTrace();
        }
        return result;
    }

    protected String getResult(AccumuloService accumuloService, String startTime,String endTime,
                               double north, double east, double south, double west)  {
        String result = "[";
        BatchScanner rawDataScanner = null;
        try {
            rawDataScanner = accumuloService.getRawDataScannerByRange(startTime,endTime);
        } catch (AccumuloSecurityException e) {
            e.printStackTrace();
        } catch (AccumuloException e) {
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            e.printStackTrace();
        }

        boolean isFirst = true;

        for (Map.Entry<Key, Value> rawDataEntry : rawDataScanner) {
            String json = rawDataEntry.getValue().toString();

            //check if tweet is in box
            JSONObject obj = null;
            try {
                obj = new JSONObject(json);
                JSONArray coords = obj.getJSONObject("coordinates").getJSONArray("coordinates");

                Double longitude = coords.getDouble(0);
                Double latitude = coords.getDouble(1);

                //TODO: does this work across meridians?
                if(west < longitude && longitude < east &&
                        south < latitude && latitude < north){

                    if(!isFirst){
                        result += ",";
                    }else{

                        isFirst=false;
                    }

                    result += json;


                }else{
                   /* System.out.println("This is should be wrong:");
                    System.out.println(west + " < " + longitude + " < "+east);
                    System.out.println(south + " < " + latitude + " < "+north);*/
                }

            }catch (JSONException e){

                //e.printStackTrace();
                //do nothing if tweet has no geotag
            }
        }
        rawDataScanner.close();
        return result + "]";
    }

    /**
     * Validates the Query parameters. Returns true, if parameters are valid, false if not.
     * @return true, if parameters are valid, false if not
     */
    private boolean validateQueryParams()
    {
        boolean consistent = true;

        long minTimestamp = 1451606400; //unix time in sec 01.01.2016 00:00 Uhr

        if(_paramNorthCoordinate == null || Objects.equals(_paramNorthCoordinate, "") || _paramNorthCoordinate.length() < 2 || !isFloat(_paramNorthCoordinate)){
            consistent = false;
        }
        if(_paramEastCoordinate == null || Objects.equals(_paramEastCoordinate, "") || _paramEastCoordinate.length() < 2 || !isFloat(_paramEastCoordinate)){
            consistent = false;
        }
        if(_paramSouthCoordinate == null || Objects.equals(_paramSouthCoordinate, "") || _paramSouthCoordinate.length() < 2 || !isFloat(_paramSouthCoordinate)){
            consistent = false;
        }
        if(_paramWestCoordinate == null || Objects.equals(_paramWestCoordinate, "") || _paramWestCoordinate.length() < 2 || !isFloat(_paramWestCoordinate)){
            consistent = false;
        }
        long tstart = Long.parseLong(_paramStartTime);
        long tend = Long.parseLong(_paramEndTime);
        if(_paramStartTime == null || Objects.equals(_paramStartTime, "") || _paramStartTime.length() != 10 || tstart < minTimestamp){
            consistent = false;
        }
        if(_paramEndTime == null || Objects.equals(_paramEndTime, "") || _paramEndTime.length() != 10 || tend < minTimestamp){
            consistent = false;
        }
        if(tstart > tend){
            consistent = false;
        }

        return consistent;
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

        }
        return isFloat;
    }

    @ExceptionHandler
    void handleIllegalArgumentException(IllegalArgumentException e, HttpServletResponse response) throws IOException {
        response.sendError(HttpStatus.BAD_REQUEST.value(),"The given parameters are not valid. Please check api documentation for further information.");
    }
}

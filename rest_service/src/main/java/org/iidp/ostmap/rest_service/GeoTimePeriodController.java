package org.iidp.ostmap.rest_service;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

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

    @RequestMapping(
            value = "/geotemporalsearch",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE}
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
            resultList = MainController.getTestTweets();
        }

        return resultList;
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

    private boolean isFloat(String checkString){
        boolean isFloat = false;
        try {
            Float.parseFloat(checkString);
            isFloat = true;
        } catch (NumberFormatException e){

        }
        return isFloat;
    }
}

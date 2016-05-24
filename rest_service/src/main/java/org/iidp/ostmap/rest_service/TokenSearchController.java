package org.iidp.ostmap.rest_service;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Range;
import org.iidp.ostmap.commons.accumulo.AccumuloService;
import org.iidp.ostmap.rest_service.helper.JsonHelper;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Controller
@RequestMapping("/api")
public class TokenSearchController {

    static Logger log = LoggerFactory.getLogger(TokenSearchController.class);

    private String _paramCommaSeparatedFieldList,
        _paramToken;

    private int _paramLimit,
        _paramOffset;

    /**
     * Mapping method for path /tokensearch
     * @param paramCommaSeparatedFieldList
     * @param paramToken
     * @param paramTopten
     * @param paramLimit
     * @param paramOffset
     * @return the result as json
     */
    @RequestMapping(
            value = "/tokensearch",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE}
    )
    @ResponseBody
    String getTweetsByFieldsAndToken(
            @RequestParam(name = "field") String paramCommaSeparatedFieldList,
            @RequestParam(name = "token") String paramToken,
            @RequestParam(name = "topten", required = false, defaultValue = "false") Boolean paramTopten,
            @RequestParam(name = "limit", required = false, defaultValue = "0") int paramLimit,
            @RequestParam(name = "offset", required = false, defaultValue = "0") int paramOffset
            ) {
        try {
            _paramCommaSeparatedFieldList = URLDecoder.decode(paramCommaSeparatedFieldList, "UTF-8");
            _paramToken = URLDecoder.decode(paramToken, "UTF-8").toLowerCase();
            log.info("Get Request received - FieldList: " + _paramCommaSeparatedFieldList + " Token: " + _paramToken);
            System.out.println("Get Request received - FieldList: " + _paramCommaSeparatedFieldList + " Token: " + _paramToken);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Cannot decode query parameters.");
        }

        _paramLimit = paramLimit;
        _paramOffset = paramOffset;

        validateQueryParams();

        String tweets = getResultsFromAccumulo(MainController.configFilePath);

        String result = "";

        if(paramTopten){
            result = JsonHelper.createTweetsWithHashtagRanking(tweets);
        }else {
            result = JsonHelper.createTweetsWithoutHashtagRanking(tweets);
        }

        return result;
    }

    /**
     * Validates the Query parameters. Returns true, if both parameters are valid, false if not.
     */
    void validateQueryParams() throws IllegalArgumentException
    {
        boolean consistent = true;
        if(_paramCommaSeparatedFieldList == null || Objects.equals(_paramCommaSeparatedFieldList, "") || _paramCommaSeparatedFieldList.length() < 2){
            throw new IllegalArgumentException("Value of query parameter 'field' is invalid.");
        }
        String[] fieldArray = _paramCommaSeparatedFieldList.split(",");
        if(fieldArray.length > 1){
            for(String singleField : fieldArray)
            {
                if(singleField == null || Objects.equals(singleField, "") || singleField.length() < 2){
                    throw new IllegalArgumentException("One comma separated values of the query parameter 'field' is invalid.");
                }
            }
        }

        if(_paramToken == null || Objects.equals(_paramToken, "") || _paramToken.length() < 2){
            throw new IllegalArgumentException("Value of query parameter 'token' is invalid.");
        }
    }

    public String getResultsFromAccumulo(String configFilePath){
        AccumuloService accumuloService = new AccumuloService();
        String result = null;
        List<Range> rawKeys = null;
        String[] fieldArray = _paramCommaSeparatedFieldList.split(",");
        int position = 0;
        int count = 0;
        try {
            accumuloService.readConfig(configFilePath);
            result = "[";

            rawKeys = new ArrayList<>();

            for(String field:fieldArray){
                // get all results from tokenIndex to the list
                Scanner termIndexScanner = accumuloService.getTermIndexScanner(_paramToken,field);
                for (Map.Entry<Key, Value> termIndexEntry : termIndexScanner) {
                    rawKeys.add(new Range(termIndexEntry.getKey().getColumnQualifier()));
                }
                termIndexScanner.close();
            }

            boolean isFirst = true;
            if(rawKeys.size() > 0){
                BatchScanner bs = accumuloService.getRawDataBatchScanner(rawKeys);

                for (Map.Entry<Key, Value> rawDataEntry : bs) {
                    if(_paramLimit > 0){
                        //Limit the result
                        if(position >= _paramOffset && count < _paramLimit){
                            if(!isFirst){
                                result += ",";
                            }else{
                                isFirst=false;
                            }

                            String json = rawDataEntry.getValue().toString();
                            json = JsonHelper.generateCoordinates(json);
                            result += json;
                            count++;
                        }else {
                            continue;
                        }

                    }else {
                        //Get all data
                        if(!isFirst){
                            result += ",";
                        }else{
                            isFirst=false;
                        }

                        String json = rawDataEntry.getValue().toString();
                        json = JsonHelper.generateCoordinates(json);
                        result += json;
                    }

                    position++;
                }
                bs.close();
            }

            result += "]";

        } catch (IOException | AccumuloSecurityException | TableNotFoundException | AccumuloException e){
            throw new RuntimeException("There was a failure during Accumulo communication.",e);
        }
        return result;
    }

    void set_paramCommaSeparatedFieldList(String _paramCommaSeparatedFieldList) {
        this._paramCommaSeparatedFieldList = _paramCommaSeparatedFieldList;
    }

    void set_paramToken(String _paramToken) {
        this._paramToken = _paramToken;
    }

    public void set_paramLimit(int _paramLimit) {
        this._paramLimit = _paramLimit;
    }

    public void set_paramOffset(int _paramOffset) {
        this._paramOffset = _paramOffset;
    }


}

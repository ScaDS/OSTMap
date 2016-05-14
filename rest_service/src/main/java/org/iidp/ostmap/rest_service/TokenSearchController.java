package org.iidp.ostmap.rest_service;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Range;
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

@Controller
@RequestMapping("/api")
public class TokenSearchController {

    private String _paramCommaSeparatedFieldList,
        _paramToken;

    /**
     * Mapping method for path /tokensearch
     * @param paramCommaSeparatedFieldList
     * @param paramToken
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
            @RequestParam(name = "token") String paramToken
            ) {
        try {
            _paramCommaSeparatedFieldList = URLDecoder.decode(paramCommaSeparatedFieldList, "UTF-8");
            _paramToken = URLDecoder.decode(paramToken, "UTF-8").toLowerCase();
            System.out.println("Get Request received - FieldList: " + _paramCommaSeparatedFieldList + " Token: " + _paramToken);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Cannot decode query parameters.");
        }

        validateQueryParams();

        return getResultsFromAccumulo(MainController.configFilePath);
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
        try {
            accumuloService.readConfig(configFilePath);
            result = "[";

            rawKeys = new ArrayList<>();

            for(String field:fieldArray){
                // get all results from tokenIndex to the list
                Scanner termIndexScanner = accumuloService.getTermIdexScanner(_paramToken,field);
                for (Map.Entry<Key, Value> termIndexEntry : termIndexScanner) {
                    rawKeys.add(new Range(termIndexEntry.getKey().getColumnQualifier()));
                }
                termIndexScanner.close();
            }

            boolean isFirst = true;
            if(rawKeys.size() > 0){
                BatchScanner bs = accumuloService.getRawDataBatchScanner(rawKeys);

                for (Map.Entry<Key, Value> rawDataEntry : bs) {

                    if(!isFirst){
                        result += ",";
                    }else{
                        isFirst=false;
                    }

                    String json = rawDataEntry.getValue().toString();
                    result += json;
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


}

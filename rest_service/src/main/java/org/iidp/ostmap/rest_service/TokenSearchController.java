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
            paramCommaSeparatedFieldList = URLDecoder.decode(paramCommaSeparatedFieldList, "UTF-8");
            paramToken = URLDecoder.decode(paramToken, "UTF-8").toLowerCase();
            log.info("Get Request received - FieldList: " + paramCommaSeparatedFieldList + " Token: " + paramToken);
            System.out.println("Get Request received - FieldList: " + paramCommaSeparatedFieldList + " Token: " + paramToken);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Cannot decode query parameters.");
        }

        validateQueryParams(paramCommaSeparatedFieldList,paramToken);

        return getResultsFromAccumulo(paramCommaSeparatedFieldList,paramToken,MainController.configFilePath);
    }

    /**
     * Validates the Query parameters. Returns true, if both parameters are valid, false if not.
     */
    void validateQueryParams(String fields, String token) throws IllegalArgumentException
    {
        boolean consistent = true;
        if(fields == null || Objects.equals(fields, "") || fields.length() < 2){
            throw new IllegalArgumentException("Value of query parameter 'field' is invalid.");
        }
        String[] fieldArray = fields.split(",");
        if(fieldArray.length > 1){
            for(String singleField : fieldArray)
            {
                if(singleField == null || Objects.equals(singleField, "") || singleField.length() < 2){
                    throw new IllegalArgumentException("One comma separated values of the query parameter 'field' is invalid.");
                }
            }
        }

        if(token == null || Objects.equals(token, "") || token.length() < 2){
            throw new IllegalArgumentException("Value of query parameter 'token' is invalid.");
        }
    }

    public String getResultsFromAccumulo(String fields, String token, String configFilePath){
        AccumuloService accumuloService = new AccumuloService();
        String result = null;
        List<Range> rawKeys = null;
        String[] fieldArray = fields.split(",");
        try {
            accumuloService.readConfig(configFilePath);
            result = "[";

            rawKeys = new ArrayList<>();

            for(String field:fieldArray){
                // get all results from tokenIndex to the list
                Scanner termIndexScanner = accumuloService.getTermIndexScanner(token,field);
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
                    json = JsonHelper.generateCoordinates(json);
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
}

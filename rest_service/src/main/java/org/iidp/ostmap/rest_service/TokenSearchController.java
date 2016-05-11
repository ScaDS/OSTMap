package org.iidp.ostmap.rest_service;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import javax.servlet.http.HttpServletResponse;
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
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException();
        }


        String resultList = "";
        if(validateQueryParams())
        {
            resultList = getResultsFromAccumulo();
        }else{
            throw new IllegalArgumentException();
        }
        return resultList;
    }

    /**
     * Validates the Query parameters. Returns true, if both parameters are valid, false if not.
     * @return true, in both parameters are valid, false if not
     */
    private boolean validateQueryParams()
    {
        boolean consistent = true;
        if(_paramCommaSeparatedFieldList == null || Objects.equals(_paramCommaSeparatedFieldList, "") || _paramCommaSeparatedFieldList.length() < 2){
            consistent = false;
        }
        String[] fieldArray = _paramCommaSeparatedFieldList.split(",");
        if(fieldArray.length > 1){
            for(String singleField : fieldArray)
            {
                if(singleField == null || Objects.equals(singleField, "") || singleField.length() < 2){
                    consistent = false;
                }
            }
        }

        if(_paramToken == null || Objects.equals(_paramToken, "") || _paramToken.length() < 2){
            consistent = false;
        }

        return consistent;
    }

    public String getResultsFromAccumulo(){
        AccumuloService accumuloService = new AccumuloService();
        String result = null;
        String[] fieldArray = _paramCommaSeparatedFieldList.split(",");
        try {
            accumuloService.readConfig(MainController.configFilePath);

            result = getResult(accumuloService, fieldArray,_paramToken);

        } catch (IOException ioe){
            ioe.printStackTrace();
        } catch (AccumuloSecurityException e) {
            e.printStackTrace();
        } catch (TableNotFoundException e) {
            e.printStackTrace();
        } catch (AccumuloException e) {
            e.printStackTrace();
        }
        return result;
    }

    protected String getResult(AccumuloService accumuloService, String[] fieldArray, String token) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        String result = "[";
        List<Range> rawKeys = new ArrayList<>();
        for(String field:fieldArray){
            // get all results from tokenIndex to the list
            Scanner termIndexScanner = accumuloService.getTermIdexScanner(token,field);
            for (Map.Entry<Key, Value> termIndexEntry : termIndexScanner) {

                rawKeys.add(new Range(termIndexEntry.getKey().getColumnQualifier()));
            }
        }

        boolean isFirst = true;
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

        return result + "]";
    }

    @ExceptionHandler
    void handleIllegalArgumentException(IllegalArgumentException e, HttpServletResponse response) throws IOException {
        response.sendError(HttpStatus.BAD_REQUEST.value(),"The given parameters are not valid. Please check api documentation for further information.");
    }
}

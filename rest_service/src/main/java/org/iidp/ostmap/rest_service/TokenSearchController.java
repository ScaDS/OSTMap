package org.iidp.ostmap.rest_service;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
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
        _paramCommaSeparatedFieldList = paramCommaSeparatedFieldList;
        _paramToken = paramToken;

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
        String result = "";
        AccumuloService accumuloService = new AccumuloService();
        String[] fieldArray = _paramCommaSeparatedFieldList.split(",");
        try {
            accumuloService.readConfig(MainController.configFilePath);
            for(String field:fieldArray){
                Scanner termIndexScanner = accumuloService.getTermIdexScanner(_paramToken,field);
                for (Map.Entry<Key, Value> termIndexEntry : termIndexScanner) {
                    String rawTwitterRowIndex = termIndexEntry.getKey().getColumnQualifierData().toString();
                    Scanner rawDataScanner = accumuloService.getRawDataScannerByRow(rawTwitterRowIndex);
                    for (Map.Entry<Key, Value> rawDataEntry : rawDataScanner) {
                        String json = rawDataEntry.getValue().toString();
                        result += json;
                    }
                }
            }
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

    @ExceptionHandler
    void handleIllegalArgumentException(IllegalArgumentException e, HttpServletResponse response) throws IOException {
        response.sendError(HttpStatus.BAD_REQUEST.value(),"The given parameters are not valid. Please check api documentation for further information.");
    }
}

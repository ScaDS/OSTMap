package org.iidp.ostmap.rest_service;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Objects;

@Controller
@RequestMapping("/api")
public class TokenSearchController {

    private String _paramCommaSeparatedFieldList,
        _paramToken;

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
            resultList = MainController.getTestTweets();
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

    @ExceptionHandler
    void handleIllegalArgumentException(IllegalArgumentException e, HttpServletResponse response) throws IOException {
        response.sendError(HttpStatus.BAD_REQUEST.value(),"The given parameters are not valid. Please check api documentation for further information.");
    }
}

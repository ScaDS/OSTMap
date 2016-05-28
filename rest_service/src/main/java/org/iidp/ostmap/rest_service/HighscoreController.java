package org.iidp.ostmap.rest_service;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.iidp.ostmap.rest_service.helper.JsonHelper;
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
public class HighscoreController {

    static Logger log = LoggerFactory.getLogger(HighscoreController.class);

    /**
     * Mapping method for path /highscore
     *
     * @return a json response
     */
    @RequestMapping(
            value = "/highscore",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_UTF8_VALUE}
    )
    @ResponseBody
    String getTweetsByGeoAndTime(
    ) throws AccumuloException, TableNotFoundException, AccumuloSecurityException, IOException {
        log.debug("HighscoreQuery #################################");

        // build query
//        GeoTempQuery geoTempQuery = new GeoTempQuery(
//                MainController.configFilePath);


        return "test";
    }
}

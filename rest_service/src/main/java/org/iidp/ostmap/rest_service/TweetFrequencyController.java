package org.iidp.ostmap.rest_service;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.iidp.ostmap.commons.accumulo.AccumuloService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;


@Controller
@RequestMapping("/api")
public class TweetFrequencyController {

    static Logger log = LoggerFactory.getLogger(TweetFrequencyController.class);
    public static DateTimeFormatter minuteFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm").withLocale(Locale.ENGLISH);

    /**
     * Mapping method for path /tweetfrequency
     *
     * @param tStart
     * @param tEnd
     * @return the result as json
     */
    @RequestMapping(
            value = "/tweetfrequency",
            method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE}
    )
    @ResponseBody
    String getTweetFrequency(
            @RequestParam(name = "tstart") String tStart,
            @RequestParam(name = "tend") String tEnd
    ) {
        String startTime;
        String endTime;
        try {
            startTime = URLDecoder.decode(tStart, "UTF-8");
            endTime = URLDecoder.decode(tEnd, "UTF-8");
            log.info("tweet frequency request:" + startTime + " to " + endTime);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Cannot decode query parameters.");
        }

        validateQueryParams(startTime, endTime);

        return getResultsFromAccumulo(startTime, endTime);
    }

    /**
     * Validates the Query parameters. throws IllegalArgumentException
     */
    void validateQueryParams(String startTime, String endTime) throws IllegalArgumentException {
        if (startTime == null || startTime.equals("") || startTime.length() != 12) {
            throw new IllegalArgumentException("Value of query parameter 'tStart' is invalid.");
        }
        if (endTime == null || endTime.equals("") || endTime.length() != 12) {
            throw new IllegalArgumentException("Value of query parameter 'tEnd' is invalid.");
        }
    }

    public String getResultsFromAccumulo(String startTime, String endTime) {
        long st = System.nanoTime();
        AccumuloService accumuloService = new AccumuloService();
        String result = null;
        try {
            accumuloService.readConfig(MainController.configFilePath);

            // get all languages
            Scanner tweetFrequencyScanner = accumuloService.getTweetFrequencyScanner(startTime, endTime, null);
            Set<String> languages = new HashSet<>();
            for (Map.Entry<Key, Value> kv : tweetFrequencyScanner) {
                languages.add(kv.getKey().getColumnFamily().toString());
            }
            tweetFrequencyScanner.close();

            if (log.isDebugEnabled()) {
                String debugOut = "";
                for (String l : languages) {
                    debugOut += l + ",";
                }
                log.debug("languages: " + debugOut);
            }

            LocalDateTime endLdt = LocalDateTime.parse(endTime, minuteFormatter);
            Map<String, List<Integer>> tweetFrequency = new HashMap<>();
            // query each language
            for (String language : languages) {
                List<Integer> frequencies = new ArrayList<>();
                Scanner languageFrequencyScanner = accumuloService.getTweetFrequencyScanner(startTime, endTime, language);
                LocalDateTime currentLdt = LocalDateTime.parse(startTime, minuteFormatter);
                //String currentTime = startTime;
                log.debug("query for: [" + startTime + ", " + endTime + ", " + language + "]");
                for (Map.Entry<Key, Value> kv : languageFrequencyScanner) {

                    // get read timestamp and data
                    String readTime = kv.getKey().getRow().toString();
                    LocalDateTime readLdt = LocalDateTime.parse(readTime, minuteFormatter);
                    Integer readFrequency = Integer.parseInt(kv.getValue().toString());
                    // fill up until read value
                    while (currentLdt.compareTo(readLdt) < 0) {
                        frequencies.add(0);
                        currentLdt = currentLdt.plusMinutes(1);
                    }
                    // insert read value
                    frequencies.add(readFrequency);
                    currentLdt = readLdt.plusMinutes(1);
                }
                while (currentLdt.compareTo(endLdt) <= 0) {
                    frequencies.add(0);
                    currentLdt = currentLdt.plusMinutes(1);
                }
                languageFrequencyScanner.close();
                // add the language to the map
                tweetFrequency.put(language, frequencies);
            }
            result = buildJsonString(tweetFrequency);

            long et = System.nanoTime();
            log.debug("time: " + ((et - st) / 1000000000.0f) + "s");
        } catch (IOException | AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
            throw new RuntimeException("There was a failure during Accumulo communication.", e);
        }
        return result;
    }

    /**
     * {
     * "data":	{
     * "en":[34,44,32,45],
     * "de":[10,11,0,12]
     * }
     * }
     *
     * @param tweetFrequency
     * @return
     */
    public String buildJsonString(Map<String, List<Integer>> tweetFrequency) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"data\":{");
        boolean isFirst = true;
        for (Map.Entry<String, List<Integer>> kv : tweetFrequency.entrySet()) {
            if (!isFirst) {
                sb.append(",");
            } else {
                isFirst = false;
            }

            sb.append("\"");
            sb.append(kv.getKey());
            sb.append("\":[");
            boolean innerFirst = true;
            for (Integer frq : kv.getValue()) {
                if (!innerFirst) {
                    sb.append(",");
                } else {
                    innerFirst = false;
                }
                sb.append(frq);
            }
            sb.append("]");
        }
        sb.append("}}");
        return sb.toString();
    }

}

package org.iidp.ostmap.rest_service;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.iidp.ostmap.commons.enums.TableIdentifier;
import org.iidp.ostmap.rest_service.helper.JsonHelper;
import org.mortbay.util.ajax.JSON;
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
import java.util.Vector;

@Controller
@RequestMapping("/api")
public class HighscoreController {

    static Logger log = LoggerFactory.getLogger(HighscoreController.class);
    public static final String PROPERTY_INSTANCE = "accumulo.instance";
    private String accumuloInstanceName;
    public static final String PROPERTY_USER = "accumulo.user";
    private String accumuloUser;
    public static final String PROPERTY_PASSWORD = "accumulo.password";
    private String accumuloPassword;
    public static final String PROPERTY_ZOOKEEPER = "accumulo.zookeeper";
    private String accumuloZookeeper;
    public static final String inTable = "HighScore";
    private Job job;

    public DataSet<Tuple2<Key,Value>> getDataFromAccumulo(ExecutionEnvironment env) throws IOException, AccumuloSecurityException {
        job = Job.getInstance(new Configuration(), "HighScoreJob");
        AccumuloInputFormat.setConnectorInfo(job, accumuloUser, new PasswordToken(accumuloPassword));
        AccumuloInputFormat.setScanAuthorizations(job, new Authorizations("standard"));
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.withInstance(accumuloInstanceName);
        clientConfig.withZkHosts(accumuloZookeeper);
        AccumuloInputFormat.setZooKeeperInstance(job, clientConfig);
        AccumuloInputFormat.setInputTableName(job, inTable);
        return env.createHadoopInput(new AccumuloInputFormat(),Key.class,Value.class, job);
    }

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
    String getHighscore(
    ) throws AccumuloException, TableNotFoundException, AccumuloSecurityException, IOException, JSONException {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Key,Value>> rawTwitterDataRows = getDataFromAccumulo(env);
        //TODO remove
        //path
        String us1 = "{\"user\":\"Zorne\",\"distance\":7589.900654023497,\"coordinates\":[[49,26.546974],[27.2147884,38.4614716],[-0.2147884,-0.4614716]]}";
        String us2 = "{\"user\":\"Falk\",\"distance\":5818.530021620169,\"coordinates\":[[0,0],[20,0],[10,20],[10,10],[10,10],[10,10],[10,10]]}";
        String us3 = "{\"user\":\"Peter\",\"distance\":4925.676750216901,\"coordinates\":[[-3.2147884,53.4614716],[47.2147884,28.4614716]]}";
        String us4 = "{\"user\":\"Oliver\",\"distance\":1062.1904270002783,\"coordinates\":[[-3.2147884,53.4614716],[-4.2147884,54.4614716],[-5.2147884,55.4614716],[-2.2147884,52.4614716],[-8.2147884,53.4614716]]}";
        //area
        String us5 = "{\"user\":\"Zorne\",\"area\":6174893.727618582,\"coordinates\":[[-0.2147884,-0.4614716],[27.2147884,38.4614716],[49,26.546974]]}";
        String us6 = "{\"user\":\"Falk\",\"area\":2465661.8600413124,\"coordinates\":[[0,0],[10,10],[10,20],[20,0]]}";
        String us7 = "{\"user\":\"Oliver\",\"area\":54980.275141825,\"coordinates\":[[-2.2147884,52.4614716],[-4.2147884,54.4614716],[-5.2147884,55.4614716],[-8.2147884,53.4614716]]}";

        JSONObject toReturn = new JSONObject();
        JSONArray areaHighscore = new JSONArray();
        JSONArray pathHighscore = new JSONArray();
        areaHighscore.put(new JSONObject(us5));
        areaHighscore.put(new JSONObject(us6));
        areaHighscore.put(new JSONObject(us7));
        pathHighscore.put(new JSONObject(us1));
        pathHighscore.put(new JSONObject(us2));
        pathHighscore.put(new JSONObject(us3));
        pathHighscore.put(new JSONObject(us4));
        toReturn.put("area",areaHighscore);
        toReturn.put("path",pathHighscore);
        log.debug("HighscoreQuery #################################");

        // build query
//        GeoTempQuery geoTempQuery = new GeoTempQuery(
//                MainController.configFilePath);


        return toReturn.toString();
    }
}

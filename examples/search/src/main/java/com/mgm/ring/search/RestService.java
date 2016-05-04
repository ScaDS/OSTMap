package com.mgm.ring.search;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.GrepIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * simple RESTservice for querying accumulo with the grep iterator
 *
 * @author Martin Grimmer (martin.grimmer@mgm-tp.com)
 */
@Controller
@SpringBootApplication
public class RestService {

    public static final String PROPERTY_INSTANCE = "accumulo.instance";
    private String accumuloInstanceName;
    public static final String PROPERTY_USER = "accumulo.user";
    private String accumuloUser;
    public static final String PROPERTY_PASSWORD = "accumulo.password";
    private String accumuloPassword;
    public static final String PROPERTY_ZOOKEEPER = "accumulo.zookeeper";
    private String accumuloZookeeper;
    private String table;
    private static final byte[] COLUMN_FAMILY = "t".getBytes();
    private static final byte[] EMPTY_BYTES = new byte[0];
    private static Logger log = Logger.getLogger(RestService.class);

    public RestService() throws IOException {
        readConfig("/var/lib/accumulo/connection.info");
    }

    /**
     * parses the config file at the given position for the necessary parameter
     *
     * @param path
     * @throws IOException
     */
    private void readConfig(String path) throws IOException {
        Properties props = new Properties();
        FileInputStream fis = new FileInputStream(path);
        props.load(fis);
        accumuloInstanceName = props.getProperty(PROPERTY_INSTANCE);
        accumuloUser = props.getProperty(PROPERTY_USER);
        accumuloPassword = props.getProperty(PROPERTY_PASSWORD);
        accumuloZookeeper = props.getProperty(PROPERTY_ZOOKEEPER);
    }

    /**
     * builds a accumulo connector
     *
     * @return the ready to use connector
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     */
    private Connector getConnector() throws AccumuloSecurityException, AccumuloException {
        // build the accumulo connector
        Instance inst = new ZooKeeperInstance(accumuloInstanceName, accumuloZookeeper);
        Connector conn = inst.getConnector(accumuloUser, new PasswordToken(accumuloPassword));
        Authorizations auths = new Authorizations("standard");
        conn.securityOperations().changeUserAuthorizations("root", auths);
        return conn;
    }

    /**
     * builds a ready to use accumulo scanner with a configured grep iterator for the given text
     *
     * @param text used as grep iterator filter
     * @return the ready to use scanner
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     * @throws TableNotFoundException
     */
    private Scanner prepareScanner(String text) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Connector conn = getConnector();
        Authorizations auths = new Authorizations("standard");
        Scanner scan = conn.createScanner("RawTwitterData", auths);
        scan.fetchColumnFamily(new Text(COLUMN_FAMILY));
        IteratorSetting grepIterSetting = new IteratorSetting(5, "grepIter", GrepIterator.class);
        GrepIterator.setTerm(grepIterSetting, text);
        scan.addScanIterator(grepIterSetting);
        return scan;
    }


    @RequestMapping("/search/")
    @ResponseBody
    String search(@RequestParam(value = "text") String text) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        String result = "";
        // only start search when there is some text given
        if (text != null && text != "") {
            Scanner scan = prepareScanner(text);
            // build the result by iterating over all results from accumulo
            for (Map.Entry<Key, Value> entry : scan) {
                String json = entry.getValue().toString();
                result += json;
            }
        }
        return result;
    }

    /**
     * app entry point
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        SpringApplication.run(RestService.class, args);
    }
}

package org.iidp.ostmap.commons;
/**
 * Created by hans on 29.04.16.
 */
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.iidp.ostmap.batch_processing.Converter;
import org.iidp.ostmap.batch_processing.Driver;
import org.iidp.ostmap.commons.accumulo.AmcHelper;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DriverTest {
    @ClassRule
    public static TemporaryFolder tmpDir = new TemporaryFolder();
    public static TemporaryFolder tmpSettingsDir = new TemporaryFolder();
    public static AmcHelper amc = new AmcHelper();

    public DriverTest() throws IOException {
    }

    @BeforeClass
    public static void setUpCluster() {

        amc.startMiniCluster(tmpDir.getRoot().getAbsolutePath());
    }

    @AfterClass
    public static void shutDownCluster() {

        amc.stopMiniCluster();
    }

    @Test
    public void testDriver() throws Exception {
        tmpSettingsDir.create();
        File settings = tmpSettingsDir.newFile("settings");

        Connector conn = amc.getConnector();
        System.out.println("I am connected as: " + conn.whoami());

        conn.tableOperations().create("RawTwitterData");
        conn.tableOperations().create("TermIndex");


        Mutation m1 = new Mutation("row1");


        m1.put("CF", "CQ", "{\n" +
                "      \"text\": \"RT @PostGradProblem: In preparation for the NFL lockout, I will be spending twice as much time analyzing my fantasy baseball team during ...\", \n\"user\": {\n" +
                "            \"notifications\": null, \n" +
                "            \"profile_use_background_image\": true, \n" +
                "            \"statuses_count\": 351, \n" +
                "            \"profile_background_color\": \"C0DEED\", \n" +
                "            \"followers_count\": 48, \n" +
                "            \"profile_image_url\": \"http://a1.twimg.com/profile_images/455128973/gCsVUnofNqqyd6tdOGevROvko1_500_normal.jpg\", \n" +
                "            \"listed_count\": 0, \n" +
                "            \"profile_background_image_url\": \"http://a3.twimg.com/a/1300479984/images/themes/theme1/bg.png\", \n" +
                "            \"description\": \"watcha doin in my waters?\", \n" +
                "            \"screen_name\": \"OldGREG85\", \n" +
                "            \"default_profile\": true, \n" +
                "            \"verified\": false, \n" +
                "            \"time_zone\": \"Hawaii\", \n" +
                "            \"profile_text_color\": \"333333\", \n" +
                "            \"is_translator\": false, \n" +
                "            \"profile_sidebar_fill_color\": \"DDEEF6\", \n" +
                "            \"location\": \"Texas\", \n" +
                "            \"id_str\": \"80177619\", \n" +
                "            \"default_profile_image\": false, \n" +
                "            \"profile_background_tile\": false, \n" +
                "            \"lang\": \"en\", \n" +
                "            \"friends_count\": 81, \n" +
                "            \"protected\": false, \n" +
                "            \"favourites_count\": 0, \n" +
                "            \"created_at\": \"Tue Oct 06 01:13:17 +0000 2009\", \n" +
                "            \"profile_link_color\": \"0084B4\", \n" +
                "            \"name\": \"GG\", \n" +
                "            \"show_all_inline_media\": false, \n" +
                "            \"follow_request_sent\": null, \n" +
                "            \"geo_enabled\": false, \n" +
                "            \"profile_sidebar_border_color\": \"C0DEED\", \n" +
                "            \"url\": null, \n" +
                "            \"id\": 80177619, \n" +
                "            \"contributors_enabled\": false, \n" +
                "            \"following\": null, \n" +
                "            \"utc_offset\": -36000\n" +
                "      }}");
        BatchWriter bw = conn.createBatchWriter("RawTwitterData", new BatchWriterConfig());
        bw.addMutation(m1);
        bw.close();



        FileOutputStream fos = new FileOutputStream(settings, false);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fos));

        String parameters = "accumulo.instance=" + conn.getInstance().getInstanceName() + "\n"+
                "accumulo.user=" + conn.whoami() +"\n"+
                "accumulo.password=password\n"+
                "accumulo.zookeeper=" + conn.getInstance().getZooKeepers();

        System.out.println(parameters);
        br.write(parameters);
        br.flush();
        br.close();
        fos.flush();
        fos.close();

        Driver d = new Driver();
        d.run(settings.getAbsolutePath());

        System.out.println("RawTwitterData: -----------------------------------------------------");

        Scanner s = conn.createScanner("RawTwitterData", new Authorizations("a"));
        for(Map.Entry<Key, Value> entry: s){
            System.out.println(entry.getKey() + " | " +entry.getValue());
            //assertEquals(entry.getValue().toString(), testString);
        }

        s.close();

        System.out.println("TermIndex: -----------------------------------------------------");
         s = conn.createScanner("TermIndex", new Authorizations("a"));
        for(Map.Entry<Key, Value> entry: s){
            System.out.println(entry.getKey() + " | " +entry.getValue());
            //assertEquals(entry.getValue().toString(), testString);
        }

        s.close();


    }

}

package org.iidp.ostmap.batch_processing.exporter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.iidp.ostmap.commons.accumulo.FlinkEnvManager;
import org.iidp.ostmap.commons.enums.TableIdentifier;

/**
 * @author Martin Grimmer (martin.grimmer@mgm-tp.com)
 */
public class Exporter {

    public static final String JOB_NAME = "data export";

    /**
     * run export process
     *
     * @param configPath path to config file
     * @param outputPath path to config file
     * @throws Exception
     */
    public void run(String configPath, String outputPath) throws Exception {

        FlinkEnvManager fem = new FlinkEnvManager(configPath, JOB_NAME,
                TableIdentifier.RAW_TWITTER_DATA.get(),
                null);

        DataSet<Tuple2<Key, Value>> rawTweetRows = fem.getDataFromAccumulo();

        DataSet<String> jsonStrings = rawTweetRows
                .map(new KVToString()).name("K/V -> Stiring");

        jsonStrings.writeAsText(outputPath).name("write to disk");

        fem.getExecutionEnvironment().execute(JOB_NAME);

    }

    /**
     * entry point
     *
     * @param args 1st parameter is used as path to accumulo config file, second parameter is output path
     */
    public static void main(String[] args) throws Exception {

        Exporter d = new Exporter();
        d.run(args[0], args[1]);

    }

}

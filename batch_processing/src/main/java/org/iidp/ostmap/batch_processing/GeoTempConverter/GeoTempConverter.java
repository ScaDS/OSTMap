package org.iidp.ostmap.batch_processing.GeoTempConverter;


import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.Text;
import org.iidp.ostmap.batch_processing.converter.ConverterProcess;
import org.iidp.ostmap.commons.accumulo.FlinkEnvManager;
import org.iidp.ostmap.commons.enums.TableIdentifier;

public class GeoTempConverter {

    /**
     * run conversion process
     * @param configPath path to config file
     * @throws Exception
     */
    public void run(String configPath) throws Exception {

        FlinkEnvManager fem = new FlinkEnvManager(configPath, "GeoTimeConvJob",
                TableIdentifier.RAW_TWITTER_DATA.get(),
                TableIdentifier.GEO_TEMPORAL_INDEX.get());

        DataSet<Tuple2<Key,Value>> rawTwitterDataRows = fem.getDataFromAccumulo();

        DataSet<Tuple2<Text, Mutation>> geoTempMutations = rawTwitterDataRows
                .flatMap(new GeoTempFlatMap(TableIdentifier.GEO_TEMPORAL_INDEX.get()));

        geoTempMutations.output(fem.getHadoopOF());

        fem.getExecutionEnvironment().execute("GeoTimeConvProcess");

    }

    /**
     * entry point
     *
     * @param args 1st parameter is used as path to config file
     */
    public static void main(String[] args) throws Exception {

        ConverterProcess d = new ConverterProcess();
        d.run(args[0]);

    }
}

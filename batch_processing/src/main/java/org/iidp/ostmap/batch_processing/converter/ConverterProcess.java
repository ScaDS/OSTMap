package org.iidp.ostmap.batch_processing.converter;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.Text;
import org.iidp.ostmap.commons.accumulo.FlinkEnvManager;
import org.iidp.ostmap.commons.enums.TableIdentifier;
import org.iidp.ostmap.commons.tokenizer.Tokenizer;


/**
 * converts data from RawTwitterTable to new format and saves it to TermIndex
 *
 */
public class ConverterProcess {


    /**
     * run conversion process
     * @param configPath path to config file
     * @throws Exception
     */
    public void run(String configPath) throws Exception {

        FlinkEnvManager fem = new FlinkEnvManager(configPath, "converterJob",
                TableIdentifier.RAW_TWITTER_DATA.get(),
                TableIdentifier.TERM_INDEX.get());

        DataSet<Tuple2<Key,Value>> rawTwitterDataRows = fem.getDataFromAccumulo();

        DataSet<Tuple2<Text, Mutation>> termIndexMutations = rawTwitterDataRows
                .flatMap(new ConverterFlatMap(new Tokenizer(),
                        TableIdentifier.TERM_INDEX.get()));

        termIndexMutations.output(fem.getHadoopOF());

        fem.getExecutionEnvironment().execute("ConverterProcess");

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
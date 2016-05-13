package org.iidp.ostmap.batch_processing.tweetPerUserAnalysis;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.iidp.ostmap.commons.accumulo.FlinkEnvManager;
import org.iidp.ostmap.commons.enums.TableIdentifier;


/**
 * writes csv containg a count of tweets per user in a given timeframe
 *
 */
public class TPUAnalysis {


    /**
     *
     * @param configPath
     * @param outputPath path for csv export
     * @param timeMin
     * @param timeMax
     * @param limit first n element only, if 0 don't filter
     * @throws Exception
     */
    public void run(String configPath, String outputPath, long timeMin, long timeMax, int limit)throws Exception {

        FlinkEnvManager fem = new FlinkEnvManager(configPath, "converterJob",
                TableIdentifier.RAW_TWITTER_DATA.get(),null);

        DataSet<Tuple2<Key,Value>> rawTwitterDataRows = fem.getDataFromAccumulo();;

        DataSet<Tuple2<String,Integer>> tweetCount = rawTwitterDataRows
                .filter(new TimeFilter(timeMin, timeMax))
                .map(new UserTweetMap())
                .groupBy(0)
                .sum(1);

        tweetCount =tweetCount.sortPartition(1, Order.DESCENDING)
                .setParallelism(1);

        if(limit >0){
            tweetCount = tweetCount.first(limit).setParallelism(1);
        }


        tweetCount.writeAsCsv(outputPath).setParallelism(1);
        //tweetCount.writeAsCsv(outputPath);


        fem.getExecutionEnvironment().execute("TPUAnalysis");

    }




    /**
     * entry point
     *
     * @param args pathToConfig, pathCSV, longTimeMin, longTimeMax (,limitFirstN)
     */
    public static void main(String[] args) throws Exception {

        TPUAnalysis d = new TPUAnalysis();
        if(args.length >4){
            d.run(args[0], args[1], Integer.parseInt(args[2]),
                    Integer.parseInt(args[3]), Integer.parseInt(args[4]));

        }else{
            d.run(args[0], args[1], Integer.parseInt(args[2]),
                    Integer.parseInt(args[3]), 0);
        }

    }

}
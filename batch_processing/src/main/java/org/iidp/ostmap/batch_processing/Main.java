package org.iidp.ostmap.batch_processing;

import org.iidp.ostmap.batch_processing.GeoTempConverter.GeoTempConverter;
import org.iidp.ostmap.batch_processing.converter.ConverterProcess;
import org.iidp.ostmap.batch_processing.tweetPerUserAnalysis.TPUAnalysis;

import java.util.Arrays;

public class Main {

    public static void main(String[] args){
        if(args.length < 1){
            System.out.println("Please use the following syntax:\n"
                    +"batch_process_name arg1 arg2 ...\n"
                    +"the following processes are available:\n"
                    +"\tconverter \n"
                    +"\t\tconverts RawTwitterData to TermIndex\n"
                    +"\t\targuments: pathToConfig\n"
                    +"\n\ttweetPerUser\n"
                    +"\t\tcreates CSV file with tweet count per user in timerange\n"
                    +"\t\targuments: pathToConfig, pathCSV, longTimeMin, longTimeMax (,limitFirstN)\n"
                    +"\n\tgeoTempConverter\n"
                    +"\t\tconverts RawTwitterData to GeoTemporalIndex\n"
                    +"\t\targuments: pathToConfig");
            return;
        }

        switch (args[0]){
            case "converter":
                try {
                    ConverterProcess.main(Arrays.copyOfRange(args, 1,args.length));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case "tweetPerUser":
                try {
                    TPUAnalysis.main(Arrays.copyOfRange(args, 1,args.length));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            case "geoTempConverter":
                try {
                    GeoTempConverter.main(Arrays.copyOfRange(args, 1,args.length));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
        }
    }
}

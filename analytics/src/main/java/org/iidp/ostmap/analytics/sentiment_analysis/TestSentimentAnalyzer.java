//package org.iidp.ostmap.analytics.sentiment_analysis;
//
//import org.apache.accumulo.core.client.*;
//import org.apache.accumulo.core.data.Key;
//import org.apache.accumulo.core.data.Value;
//import org.iidp.ostmap.commons.accumulo.AccumuloService;
//import org.iidp.ostmap.rest_service.MainController;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import org.apache.accumulo.core.data.Range;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
//public class TestSentimentAnalyzer {
//
//    static Logger log = LoggerFactory.getLogger(TestSentimentAnalyzer.class);
//
//    private AccumuloService accumuloService;
//
//    public TestSentimentAnalyzer() throws IOException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
//        this.accumuloService = new AccumuloService();
//        String configPath = MainController.configFilePath;
//
//        this.accumuloService.readConfig(configPath);
//
//        List<Range> rangeList = new ArrayList<>();
//
//        rangeList = new ArrayList<>();
//
////        for(String field:fieldArray){
////            // get all results from tokenIndex to the list
////            Scanner termIndexScanner = accumuloService.getTermIndexScanner(token,field);
////            for (Map.Entry<Key, Value> termIndexEntry : termIndexScanner) {
////                rawKeys.add(new Range(termIndexEntry.getKey().getColumnQualifier()));
////            }
////            termIndexScanner.close();
////        }
//
//        BatchScanner batchScanner = this.accumuloService.getRawDataBatchScanner(rangeList);
//    }
//}

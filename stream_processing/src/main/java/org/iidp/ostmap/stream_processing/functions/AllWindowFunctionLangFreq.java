package org.iidp.ostmap.stream_processing.functions;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple3;

import java.util.HashMap;
import java.util.Map;

/**
 * window function for the 'language frequency count'-analysis
 */

public class AllWindowFunctionLangFreq implements AllWindowFunction<Tuple3<Long, String, String>, Tuple3<String, String, Integer>, TimeWindow> {

    @Override
    public void apply(TimeWindow window, Iterable<Tuple3<Long, String, String>> values, Collector<Tuple3<String, String, Integer>> out) throws Exception {
        Map<String, Integer> collected = new HashMap<String, Integer>();
        String lastTimestamp = null;
        for (Tuple3<Long, String, String> tuple : values) {
            String langTag = tuple._2();
            lastTimestamp = tuple._3();
            if (collected.get(langTag) == null) {
                collected.put(langTag, 1);
            } else {
                collected.put(langTag, collected.get(langTag) + 1);
            }
        }
        if (lastTimestamp != null) {
            for (Map.Entry<String, Integer> entry : collected.entrySet()) {
                out.collect(new Tuple3<>(lastTimestamp, entry.getKey(), entry.getValue()));
            }
        }
    }




}

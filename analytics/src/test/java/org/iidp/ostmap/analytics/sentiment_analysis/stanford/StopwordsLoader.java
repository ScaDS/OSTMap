package org.iidp.ostmap.analytics.sentiment_analysis.stanford;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;

public class StopwordsLoader {
    public ArrayList<String> loadstopwords(String stopWordsFileName){
//        Source.fromInputStream(getClass.getResourceAsStream("/" + stopWordsFileName)).getLines().toList;
        ClassLoader classLoader = getClass().getClassLoader();
        URL url = classLoader.getResource(stopWordsFileName);
//            System.out.println(url.getFile());
        InputStream in = this.getClass().getResourceAsStream("/" + stopWordsFileName);
//        File file = new File(url.getFile());
        InputStreamReader inr = new InputStreamReader(in);
        BufferedReader br = new BufferedReader(inr);
//        System.out.println(br.readLine());
        ArrayList<String> list = new ArrayList<String>();
        String str = null;
        try {
            while((str = br.readLine())!=null){
                list.add(str);
            }
        }
        catch(IOException e) {
            e.printStackTrace();
        }
//            System.out.println(list);
        return list;

    }
}

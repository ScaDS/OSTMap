package org.iidp.ostmap.analytics.sentiment_analysis.stanford;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

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

    /**
     * Reads a file which represents a list of stopwords.
     * @param pathToFile Path to file
     * @return List of Strings ("stopwords")
     * @throws IOException General exception for input-/ output-errors.
     */
    public static List<String> loadStopwordsFromFile(String pathToFile) throws IOException {
        List<String> stopwords = new ArrayList<>();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(pathToFile)));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            stopwords.add(line);
        }
        return stopwords;
    }
}

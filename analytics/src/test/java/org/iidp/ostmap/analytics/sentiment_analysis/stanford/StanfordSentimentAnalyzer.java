package org.iidp.ostmap.analytics.sentiment_analysis.stanford;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import twitter4j.Status;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 *
 */
public class StanfordSentimentAnalyzer {

    /**
     * todo: remove stopwords
     *      load stopwords list
     */

    /**
     *
     * @param statusList
     * @return
     */
    public Map<Long, HashMap> predictTweetSentiments(List<Status> statusList) {
        Map<Long, HashMap> resultMap = new HashMap<>();

        // set up pipeline properties
        Properties props = new Properties();

        // set the list of annotators to run
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma,parse,sentiment");

        // build pipeline
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

        Annotation document;

        for (Status status : statusList) {

            String tweet = this.removeLineFeed(status.getText());
            tweet = this.removeHttp(tweet);

            // create an empty annotation just with the given text
            document = new Annotation(status.getText());

            // annnotate the document
            pipeline.annotate(document);

            // these are all the sentences in this document
            // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
            List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

            Map<Annotation, Integer> sentenceMap = new HashMap<>();

            for(CoreMap sentence: sentences) {

                // this is the parse tree of the current sentence
                Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);

                /**
                 * RNNCoreAnnotations.getPredictedClass
                 * 0 = very negative
                 * 1 = negative
                 * 2 = neutral
                 * 3 = positive
                 * 4 = very positive
                 */
                Integer sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                sentenceMap.put((Annotation) sentence, sentiment);

            }
            resultMap.put(status.getId(), (HashMap) sentenceMap);
        }

        return resultMap;
    }

    /**
     * Removes newlines (line feed - '\n') within a tweet.
     * @param tweetText String: tweet text
     * @return tweet text without any newline
     */
    private String removeLineFeed(String tweetText) {
        return tweetText.replaceAll("\n", "");
    }

    private String removeHttp(String tweetText) {
        Pattern HTTP = Pattern.compile(
                "http.?\\s"
        );


        Matcher m = HTTP.matcher(tweetText);
        String t = "";
        StringBuilder sb = new StringBuilder(tweetText);

        while (m.find()) {
            sb.delete(m.start(), m.end());
        }

        System.out.println("Before: " + tweetText);
        System.out.println("After: " + sb);
        return t;
    }

}

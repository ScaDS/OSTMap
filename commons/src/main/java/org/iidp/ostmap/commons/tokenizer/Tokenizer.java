package org.iidp.ostmap.commons;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


/**
 * Created by CSchott on 23.04.16.
 */
public class Tokenizer {

    private List<String> separator = new ArrayList<String>();
    private List<String> doubleList = new ArrayList<String>();

    public Tokenizer(){

        separator.add(".");
        separator.add(",");
        separator.add("'");
        separator.add("!");
        separator.add("?");
        separator.add("\"");
        separator.add(";");
        separator.add(":");

        doubleList.add("#");
        doubleList.add("@");

    }

    /**
     * Function for tokenizing Strings
     * @param inputString String to be tokenized
     * @return List of Tokens
     */
    public List tokenizeString(String inputString) {

        List<String> tokenList = new ArrayList<String>();

        StringTokenizer st = new StringTokenizer(inputString);
        while (st.hasMoreTokens()) {
            String currentToken = st.nextToken();
            currentToken = currentToken.toLowerCase();

            for (String i : separator) {
                if (currentToken.contains(i)) {
                    currentToken = currentToken.replaceAll(i, "");
                }
            }

            for (String p : doubleList) {
                if (currentToken.contains(p)) {
                    String additionalHashtagToken = currentToken.replaceAll(p, "");
                    if(additionalHashtagToken.length() >= 2) {
                        tokenList.add(additionalHashtagToken);
                        //System.out.println("Added additional Token: " + additionalHashtagToken);
                    }
                }
            }
                if(currentToken.length()>=2) {
                    tokenList.add(currentToken);
                    //System.out.println("Added current Token: "+currentToken);
                }
        }
        return tokenList;


    }



}

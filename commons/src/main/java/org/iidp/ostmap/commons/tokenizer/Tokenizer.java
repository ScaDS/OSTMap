package org.iidp.ostmap.commons.tokenizer;

import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class Tokenizer implements Serializable{

    private List<String> separator = new ArrayList<String>();
    private List<String> doubleList = new ArrayList<String>();

    public Tokenizer(){

        try {
            String filePath = new File("").getAbsolutePath();
            filePath=filePath.concat("/src/main/java/org/iidp/ostmap/commons/tokenizer/tokenizerConfig.json");
            //System.out.println(filePath);

            byte[] encoded = Files.readAllBytes(Paths.get(filePath));
            String json = new String(encoded, Charset.defaultCharset());
            //System.out.println(json);

            JSONObject obj = new JSONObject(json);

            for(int i=0; i<obj.getJSONArray("separator").length();i++) {
                separator.add(obj.getJSONArray("separator").getString(i));
            }

            for (int i=0; i<obj.getJSONArray("duplicators").length();i++){
                doubleList.add(obj.getJSONArray("duplicators").getString(i));
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }


    }

    /**
     * Function for tokenizing Strings
     * @param inputString String to be tokenized
     * @return List of Tokens
     */
    public List<String> tokenizeString(String inputString){

        List<String> tokenList = new ArrayList<String>();

        StringTokenizer st = new StringTokenizer(inputString);
        while (st.hasMoreTokens()) {
            String currentToken = st.nextToken();
            currentToken = currentToken.toLowerCase();



            for (String i : separator) {

                if (currentToken.contains(i)) {
                    
                    currentToken = currentToken.replace(i, "");
                }
            }

            for (String p : doubleList) {
                if (currentToken.contains(p)) {
                    String additionalHashtagToken = currentToken.replace(p, "");
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

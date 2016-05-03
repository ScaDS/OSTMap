package org.iidp.ostmap.commons;

import org.junit.Test;

import java.util.List;

import static junit.framework.TestCase.fail;


public class TokenizerTest {

    @Test
    public void testTokenizer(){

        String s = "RT @PostGradProblem: blabla blabla ...";
        Tokenizer t = new Tokenizer();
        List<String> l = t.tokenizeString(s);
        for(String token: l){
            System.out.println("Token: " + token);
            if(token.length()<2){
                fail(); //no token should be under 2 chars
            }
        }
        if(l.size() < 5){
            fail(); //the example tweet should generate >=5 tokens
        }

    }



}

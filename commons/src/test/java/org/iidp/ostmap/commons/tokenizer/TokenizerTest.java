package org.iidp.ostmap.commons.tokenizer;

/**
 * Created by schotti on 28.04.16.
 */
import org.iidp.ostmap.commons.Tokenizer;
import org.junit.Assert;
import org.junit.Test;


import java.util.List;

public class TokenizerTest {

    private static final String INPUT_TEXT1 = "this is some text\n and i love it #truelove @test" ;
    private static final String INPUT_TEXT2 = "Hello Example ...";
    private static final String[] TOKENS1 = new String[] { "this" ,"is", "some", "text", "and" , "love", "it", "#truelove", "truelove","@test", "test"};
    private static final String[] TOKENS2 = new String[] { "hello"  ,"example" };


    @Test
    public void testGetTokens(){
        Tokenizer tokenizer = new Tokenizer();

        List<String> tokens1 = tokenizer.tokenizeString(INPUT_TEXT1);

        for(String s: tokens1){
            Boolean equal1 = false;
            for(int i= 0 ; i<TOKENS1.length;i++){
                if(s.equals(TOKENS1[i])){
                    equal1= true;
                    break;
                }
            }
            Assert.assertTrue(equal1);
        }

        List<String> tokens2 = tokenizer.tokenizeString(INPUT_TEXT2);
        for(String s: tokens2){
            Boolean equal2 = false;
            for(int i= 0 ; i<TOKENS2.length;i++){
                if(s.equals(TOKENS2[i])){
                    equal2= true;
                    break;
                }
            }
            Assert.assertTrue(equal2);
        }


    }

}

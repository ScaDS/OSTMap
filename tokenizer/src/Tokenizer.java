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
    private List tokenizeString(String inputString) {

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
                    tokenList.add(additionalHashtagToken);
                    //System.out.println("Added additional Token: " + additionalHashtagToken);
                }
            }
                tokenList.add(currentToken);
                //System.out.println("Added current Token: "+currentToken);

        }
        return tokenList;
    }

    /**
     * Example Main Method:
     * @param args
     */
    public static void main(String[] args){

        Tokenizer tokenizer = new Tokenizer();

        List<String> test = tokenizer.tokenizeString("Endlich! Am Montag präsentieren wir euch Satire-Hits für alle wichtigen Themen der nächsten fünf Jahre! #HalliGalli\n");
        /*
        for(int i=0 ; i< test.size(); i++){
            System.out.println(test.get(i));
        }
        */
    }
}

package org.iidp.ostmap.commons.enums;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class EnumTest {
    //@ClassRule

    @BeforeClass
    public static void preTest(){}

    @AfterClass
    public static void postTest(){}

    @Test
    public  void testEnums() {
        System.out.println((TableIdentifiers.RAW_TWITTER_TABLE));
        String raw = TableIdentifiers.RAW_TWITTER_TABLE.toString();
        System.out.println(raw);
        System.out.println((TableIdentifiers.TERM_INDEX));
        String term = TableIdentifiers.TERM_INDEX.toString();
        System.out.println(term);
        /**
         *  doesnt work:
         *  String raw = TableIdentifiers.RAW_TWITTER_TABLE:
         */
    }

}

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
        System.out.println((AccumuloIdentifiers.PROPERTY_USER));
        String user = AccumuloIdentifiers.PROPERTY_USER.toString();
        System.out.println(user);
        System.out.println((AccumuloIdentifiers.PROPERTY_INSTANCE));
        String instance = AccumuloIdentifiers.PROPERTY_INSTANCE.toString();
        System.out.println(instance);
        System.out.println((AccumuloIdentifiers.PROPERTY_PASSWORD));
        String password = AccumuloIdentifiers.PROPERTY_PASSWORD.toString();
        System.out.println(password);
        System.out.println((AccumuloIdentifiers.PROPERTY_ZOOKEEPER));
        String zookeeper = AccumuloIdentifiers.PROPERTY_ZOOKEEPER.toString();
        System.out.println(zookeeper);
        /**
         *  doesnt work:
         *  String raw = TableIdentifiers.RAW_TWITTER_TABLE:
         */
    }

}

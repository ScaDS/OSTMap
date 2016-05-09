package org.iidp.ostmap.commons.enums;


public enum TableIdentifiers {
    RAW_TWITTER_TABLE{
        String content = "RawTwitterTable";
        public String toString(){
            return "RawTwitterTable";
        }
    },
    TERM_INDEX{
        public String toString(){
            return "TermIndex";
        }
    }
}

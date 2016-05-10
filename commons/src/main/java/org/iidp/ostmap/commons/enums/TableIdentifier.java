package org.iidp.ostmap.commons.enums;


public enum TableIdentifier {
    RAW_TWITTER_TABLE("RawTwitterTable"),
    TERM_INDEX("TermIndex");

    String identifier = "";

    TableIdentifier(String identifier){
        this.identifier = identifier;
    }

    public String get(){
        return this.identifier;
    }

    @Override
    public String toString(){
        return "table identifier: " + identifier;
    }
}

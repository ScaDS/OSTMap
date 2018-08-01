package org.iidp.ostmap.commons.enums;


public enum TableIdentifier {
    RAW_TWITTER_DATA("ostmap.RawTwitterData"),
    TERM_INDEX("ostmap.TermIndex"),
    GEO_TEMPORAL_INDEX("ostmap.GeoTemporalIndex"),
    TWEET_FREQUENCY("ostmap.TweetFrequency"),
    SENTIMENT_DATA("ostmap.SentimentData"),
    NAMESPACE("ostmap");

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

package org.iidp.ostmap.commons.accumulo.keys;

/**
 * Contains data for one key of the termIndex-table
 */
public class TermIndexKey {

    // constants for source types
    public static final String SOURCE_TYPE_USER = "user";
    public static final String SOURCE_TYPE_TEXT = "text";

    // the row in termIndex-table
    public String term;

    // the column-family in termIndex-table (user or text)
    public String source;

    // the column-qualifier in termIndex-table which is fk for rawTwitterData
    public RawTwitterDataKey rawTwitterDataKey;

    // empty constructor
    public TermIndexKey() {}

    /**
     * builds a termIndexKey for the given parameter
     *
     * @param term      token or username
     * @param source    source-type which defines if user or token
     * @param rtdKey    the RawTwitterDataKey
     * @return          the new termIndex object
     */
    public static TermIndexKey buildTermIndexKey(String term, String source, RawTwitterDataKey rtdKey) {
        TermIndexKey key = new TermIndexKey();
        key.term = term;
        key.source = source;
        key.rawTwitterDataKey = rtdKey;
        return key;
    }

    public byte[] getTermBytes() {
        return term.getBytes();
    }

    public byte[] getSourceBytes() {
        return source.getBytes();
    }

    public void setTerm(String term) {
        this.term = term;
    }
}

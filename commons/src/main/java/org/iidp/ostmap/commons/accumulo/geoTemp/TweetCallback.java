package org.iidp.ostmap.commons.accumulo.geoTemp;

/**
 * interface for geoTemp API
 * process will be called for every result tweet in given geoTemp
 */
public interface TweetCallback {

    void process(String json);
}

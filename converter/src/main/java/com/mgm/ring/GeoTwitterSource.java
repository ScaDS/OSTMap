package com.mgm.ring;

import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Martin Grimmer (martin.grimmer@mgm-tp.com)
 */
public class GeoTwitterSource extends TwitterSource {
    private static final Logger LOG = LoggerFactory.getLogger(GeoTwitterSource.class);

    public GeoTwitterSource(String authPath) {
        super(authPath);
    }

    @Override
    protected void initializeConnection() {
        if (LOG.isInfoEnabled()) {
            LOG.info("Initializing Twitter Streaming API connection");
        }

        this.queue = new LinkedBlockingQueue(this.queueSize);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint(false)
                //.trackTerms(Arrays.asList("berlin","hamburg", "münchen", "köln", "frannkfurt", "stuttgart", "düsseldorf", "dortmund", "essen", "bremen", "leipzig", "dresden", ))

                .locations(Arrays.asList(new Location(
                        // leipzig:  12.26,51.27,12.51,51.41
                        // deutschland: 6.20,46.93,15.50,74.10
                        // europa: -32.0 34.0 40.0 75.0
                        new Location.Coordinate(-32.0, 34.0), // south west
                        new Location.Coordinate(40.0, 75.0)))); // north east
        endpoint.stallWarnings(false);
        OAuth1 auth = this.authenticate();
        this.initializeClient(endpoint, auth);
        if (LOG.isInfoEnabled()) {
            LOG.info("Twitter Streaming API connection established successfully");
        }

    }


}

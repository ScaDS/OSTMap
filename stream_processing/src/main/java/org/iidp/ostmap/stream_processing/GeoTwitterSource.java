package org.iidp.ostmap.stream_processing;

import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Source for the twitter-data
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
                .locations(Arrays.asList(
                                new Location(
                                        // europa: -32.0 34.0 40.0 75.0
                                        new Location.Coordinate(-32.0, 34.0), // south west
                                        new Location.Coordinate(40.0, 75.0))
                         //       new Location(
                         //               // north america: -168.48633, 13.23995 -50.36133, 72.76406
                         //               new Location.Coordinate(-168.48633, 13.23995), // south west
                         //               new Location.Coordinate(-50.36133, 72.76406))
                        )
                );
        endpoint.stallWarnings(false);
        OAuth1 auth = this.authenticate();
        this.initializeClient(endpoint, auth);
        if (LOG.isInfoEnabled()) {
            LOG.info("Twitter Streaming API connection established successfully");
        }

    }


}

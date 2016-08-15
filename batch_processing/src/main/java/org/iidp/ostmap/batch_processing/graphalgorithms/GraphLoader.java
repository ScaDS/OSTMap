package org.iidp.ostmap.batch_processing.graphalgorithms;

import org.apache.flink.graph.Graph;
import org.iidp.ostmap.batch_processing.graphalgorithms.datastructures.UserEdgeValues;
import org.iidp.ostmap.batch_processing.graphalgorithms.datastructures.UserNodeValues;

public class GraphLoader {


    /**
     * builds a user graph from tweets in json format
     * an user represents an vertex
     * an edge A->B is created if an user A mentions another user B
     * @param path path to folder containing json files to load as user graph
     * @return
     */
    public Graph<String, UserNodeValues, UserEdgeValues> getUserGraphFromFiles(String path) {

        return null;
    }

}

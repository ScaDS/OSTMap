package org.iidp.ostmap.batch_processing.graphprocessing.algorithms;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.ConnectedComponents;
import org.iidp.ostmap.batch_processing.graphprocessing.GraphLoader;
import org.iidp.ostmap.batch_processing.graphprocessing.datastructures.UserEdgeValues;
import org.iidp.ostmap.batch_processing.graphprocessing.datastructures.UserNodeValues;

/**
 * This class is used to get a brief overview of the graph by creating a summary with the help of the weakly connected
 * component analysis of gelly.
 */
public class ConnectionAnalysis {
    private static final Integer MAX_ITERATIONS = 100;


    /**
     * Executes the connection analysis for a given {@link Graph}.
     * @param graph the actual graph
     */
    public void analyze(Graph<String, UserNodeValues, UserEdgeValues> graph) throws Exception {
        ConnectedComponents<String, UserNodeValues, UserEdgeValues> cc = new ConnectedComponents<>(MAX_ITERATIONS);
        DataSet<Vertex<String, UserNodeValues>> ccResult = cc.run(graph);

        ccResult.groupBy(2).sum(1);
        ccResult.groupBy(2).sum(2);
    }


    public static void main(String[] args) throws Exception {
        GraphLoader gl = new GraphLoader();
        Graph<String, UserNodeValues, UserEdgeValues> graph;
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        if(args.length == 0) {
            // read graph from accumulo
            graph = null; // TODO : adapt to gl.getUserGraph();
        } else if (args.length == 1) {
            graph = gl.getUserGraphFromFiles(args[0],env);
        } else {
            throw new MissingArgumentException("Either use no arguments for graph import from accumulo or the path " +
                    "to file where the graph is stored.");
        }

        ConnectionAnalysis ca = new ConnectionAnalysis();
        ca.analyze(graph);
    }
}

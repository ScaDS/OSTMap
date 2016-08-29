package org.iidp.ostmap.batch_processing.graphprocessing.algorithms;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.GroupCombineOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.ConnectedComponents;
import org.apache.flink.util.Collector;
import org.iidp.ostmap.batch_processing.graphprocessing.GraphLoader;
import org.iidp.ostmap.batch_processing.graphprocessing.datastructures.UserEdgeValues;
import org.iidp.ostmap.batch_processing.graphprocessing.datastructures.UserNodeValues;
import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;


/**
 * This class is used to get a brief overview of the graph by creating a summary with the help of the weakly connected
 * component analysis of gelly.
 */
public class ConnectionAnalysis {
    private static final Logger logger = getLogger(ConnectionAnalysis.class);
    private static final Integer MAX_ITERATIONS = 100;


    /**
     * Executes the connection analysis for a given {@link Graph}.
     * @param graph the actual graph
     */
    public void analyze(Graph<String, UserNodeValues, UserEdgeValues> graph) throws Exception {
        ConnectedComponents<String, UserNodeValues, UserEdgeValues> cc = new ConnectedComponents<>(MAX_ITERATIONS);
        DataSet<Vertex<String, UserNodeValues>> ccResult = cc.run(graph);

        //***************************************************************************************
        // count number of weakly connected components
        //***************************************************************************************
        logger.info("Number of weakly connected components: ");
        ccResult.groupBy("V").combineGroup(new GroupCombineFunction<Vertex<String,UserNodeValues>, Long>() {
            @Override
            public void combine(Iterable<Vertex<String, UserNodeValues>> values, Collector<Long> out) throws Exception {
                out.collect(1L);
            }
        }).sum(0).print();




        //***************************************************************************************
        // print sizes of weakly connected components, sort desc
        //***************************************************************************************
        logger.info("Size of weakly connected components (sorted, desc): ");
        DataSet<Tuple2<UserNodeValues, Long>> countMap = ccResult.groupBy("V")
                .combineGroup(new GroupCombineFunction<Vertex<String, UserNodeValues>, Tuple2<UserNodeValues, Long>>() {
            @Override
            public void combine(Iterable<Vertex<String, UserNodeValues>> values, Collector<Tuple2<UserNodeValues, Long>> out) throws Exception {
                long componentSize = 0;
                UserNodeValues compIdentifier = null;
                for (Vertex<String, UserNodeValues> vertex : values) {
                    componentSize++;

                    if (compIdentifier != null && !compIdentifier.equals(vertex.getValue())) {
                        throw new IllegalArgumentException(compIdentifier + " should be equal to " + vertex.getValue());
                    } else {
                        compIdentifier = vertex.getValue();
                    }
                }
                out.collect(new Tuple2<>(compIdentifier, componentSize));
            }
        });
        countMap.sortPartition("T1", Order.DESCENDING).first(10).print();


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

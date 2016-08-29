package org.iidp.ostmap.batch_processing.graphprocessing;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Graph;
import org.apache.flink.util.Collector;
import org.codehaus.jettison.json.JSONArray;
import org.iidp.ostmap.batch_processing.graphprocessing.datastructures.UserEdgeValues;
import org.iidp.ostmap.batch_processing.graphprocessing.datastructures.UserNodeValues;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class GraphLoader {


    public static void main(String[] args) throws Exception {
        GraphLoader gl = new GraphLoader();
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //Graph<String, UserNodeValues, UserEdgeValues> graph = gl.getUserGraphFromFiles("/data/tweets/sample.txt", env);
        Graph<String, UserNodeValues, UserEdgeValues> graph = gl.getUserGraphFromFiles("/data/tweets/24", env);

        //graph.getVertices().print();
        //graph.getEdges().print();
    }


    /**
     * builds a user graph from tweets in json format
     * an user represents an vertex
     * an edge A->B is created if an user A mentions another user B
     *
     * @param path path to folder containing json files to load as user graph
     * @return
     */
    public Graph<String, UserNodeValues, UserEdgeValues> getUserGraphFromFiles(String path, ExecutionEnvironment env) {


        DataSet<String> rawData = readRawData(path, env);
        DataSet<JSONObject> jsonData = getJsonData(rawData);

        DataSet<Tuple2<String, UserNodeValues>> vertices = getUserNodes(jsonData);
        DataSet<Tuple3<String, String, UserEdgeValues>> edges = getUserEdges(jsonData);

        Graph<String, UserNodeValues, UserEdgeValues> g = Graph.fromTupleDataSet(vertices, edges, env);

        return g;
    }

    private DataSet<String> readRawData(String path, ExecutionEnvironment env) {
        DataSet<String> data = env.readTextFile(path).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if (value.length() > 1) {
                    return true;
                }
                return false;
            }
        });
        return data;
    }

    private DataSet<JSONObject> getJsonData(DataSet<String> rawData) {
        DataSet<JSONObject> jsonData = rawData.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String stringValue) throws Exception {
                JSONObject obj = null;
                try {
                    obj = new JSONObject(stringValue);
                } catch (JSONException e) {
                }
                return obj;
            }
        });
        return jsonData;
    }


    private DataSet<Tuple2<String, UserNodeValues>> getUserNodes(DataSet<JSONObject> jsonData) {
        DataSet<Tuple2<String, UserNodeValues>> userNodes = jsonData.map(new MapFunction<JSONObject, Tuple2<String, UserNodeValues>>() {
            @Override
            public Tuple2<String, UserNodeValues> map(JSONObject jsonObject) throws Exception {
                JSONObject user = jsonObject.getJSONObject("user");
                String userId = user.getString("id_str");
                String userName = user.getString("name");
                return new Tuple2<String, UserNodeValues>(userId, new UserNodeValues(userId,userName));
            }
        });
        return userNodes;
    }

    private DataSet<Tuple3<String, String, UserEdgeValues>> getUserEdges(DataSet<JSONObject> jsonData) {

        DataSet<Tuple3<String, String, UserEdgeValues>> userEdges = jsonData.flatMap(new FlatMapFunction<JSONObject, Tuple3<String, String, UserEdgeValues>>() {
            @Override
            public void flatMap(JSONObject jsonObject, Collector<Tuple3<String, String, UserEdgeValues>> out) throws Exception {
                // count initialized to 1
                int count = 1;

                // from the current node
                JSONObject user = jsonObject.getJSONObject("user");
                String from = user.getString("id_str");

                // to other nodes
                JSONObject entities = jsonObject.getJSONObject("entities");
                JSONArray userMentions = entities.getJSONArray("user_mentions");
                for (int i = 0; i < userMentions.length(); i++) {
                    JSONObject current = userMentions.getJSONObject(i);
                    String to = current.getString("id_str");
                    out.collect(new Tuple3<String, String, UserEdgeValues>(from, to, new UserEdgeValues(count)));
                }
                return;
            }
        });
        return userEdges;
    }
}

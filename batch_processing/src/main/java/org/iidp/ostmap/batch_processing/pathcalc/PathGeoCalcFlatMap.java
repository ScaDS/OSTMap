package org.iidp.ostmap.batch_processing.pathcalc;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.util.ajax.JSONObjectConvertor;

import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.function.BiConsumer;


public class PathGeoCalcFlatMap implements FlatMapFunction<Tuple2<String,String>, Tuple2<String,Double>> {


    double equatorialEarthRadius = 6378.1370;
    double deg2rad = (Math.PI / 180.);
    int decimalPlaces = 5;

    public PathGeoCalcFlatMap(){
    }

    /**
     * Calculates the area defined by the coordinates of a users tweets
     * @param in user with extracted coordinates
     * @param out User with biggest area defined by tweets
     */
    @Override
    public void flatMap(Tuple2<String, String> in, Collector<Tuple2<String, Double>> out) {

        String userName = in.f0;
        String[] tweets;
        Vector<double[]> coordinates = new Vector<>();
        TreeMap<Long,Double[]> path = new TreeMap<>();
        TreeMap<Long,Double[]> path2 = new TreeMap<>();
        double distance = 0.;

        String[] stringCoordSet;
        double area = 0.0;

        try {
            tweets = in.f1.split("\\|");
            for (String entry: tweets) {
                Double[] coords = {0.,0.};
                JSONObjectConvertor jk = new JSONObjectConvertor();
                JSONObject obj = new JSONObject(entry);
                coords[0] = obj.getJSONArray("coordinates").getJSONArray(0).getDouble(0);
                coords[1] = obj.getJSONArray("coordinates").getJSONArray(0).getDouble(1);
                path.put(obj.getJSONArray("timestamp_ms").getLong(0),coords);
            }
            path2 = (TreeMap<Long,Double[]>)path.clone();
            while(path.size()>1){
                Map.Entry<Long,Double[]> entry = path.pollFirstEntry();
                double long1 = entry.getValue()[0];
                double lat1 = entry.getValue()[1];
                double long2 = path.firstEntry().getValue()[0];
                double lat2 = path.firstEntry().getValue()[1];
                distance += this.haversineInKm(long1,lat1,long2,lat2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        JSONObject data = new JSONObject();
        try {
            data.put("user",userName);
            data.put("distance",distance);
            JSONArray coordsJSON = new JSONArray();
            while(!path2.isEmpty()){
                Map.Entry<Long,Double[]> entry = path2.pollFirstEntry();
                JSONArray newCoords = new JSONArray();
                newCoords.put(entry.getValue()[0]);
                newCoords.put(entry.getValue()[1]);
                coordsJSON.put(newCoords);
            }
            data.put("coordinates",coordsJSON);
        } catch (JSONException e) {
            e.printStackTrace();
        }

        out.collect(new Tuple2<>(data.toString(),distance));


    }


    /**
     * calculates the distance between two sets of coordinates using the haversine formula
     * @param lat1 latitude coordinate 1
     * @param long1 longitude coordinate 1
     * @param lat2 latitude coordinate 2
     * @param long2 longitude coordinate 2
     * @return the distance betweend the two coordinates
     */
    public double haversineInKm(double long1, double lat1, double long2, double lat2){
        double dlong = (long2 - long1) * deg2rad;
        double dlat = (lat2 - lat1) * deg2rad;
        double a = Math.pow(Math.sin(dlat / 2.), 2.) + Math.cos(lat1 * deg2rad) * Math.cos(lat2 * deg2rad) * Math.pow(Math.sin(dlong / 2.), 2.);
        double c = 2D * Math.atan2(Math.sqrt(a), Math.sqrt(1. - a));
        double d = equatorialEarthRadius * c;

        return d;
    }

}

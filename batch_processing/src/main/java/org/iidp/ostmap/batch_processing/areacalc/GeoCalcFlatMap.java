package org.iidp.ostmap.batch_processing.areacalc;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Vector;


public class GeoCalcFlatMap implements FlatMapFunction<Tuple2<String,String>, Tuple2<String,String>> {


    double equatorialEarthRadius = 6378.1370;
    double deg2rad = (Math.PI / 180.);
    int decimalPlaces = 5;

    public GeoCalcFlatMap(){
    }

    @Override
    public void flatMap(Tuple2<String, String> in, Collector<Tuple2<String, String>> out) {

        JSONObject obj = null;
        String userName = in.f0;
        String[] coords;
        Vector<double[]> coordinates = new Vector<>();

        String[] stringCoordSet;
        double area = 0.0;

        try {
            coords = in.f1.split("\\|");
            for (String entry: coords) {
                stringCoordSet = entry.split(",");
                double[] coordSet = {Double.parseDouble(stringCoordSet[0]),Double.parseDouble(stringCoordSet[1])};
                if(coordinates.size() < 4){
                    coordinates.add(coordSet);
                }else{
                    Vector<double[]> tempCoordinates0 = (Vector<double[]>) coordinates.clone();
                    Vector<double[]> tempCoordinates1 = (Vector<double[]>) coordinates.clone();
                    Vector<double[]> tempCoordinates2 = (Vector<double[]>) coordinates.clone();
                    Vector<double[]> tempCoordinates3 = (Vector<double[]>) coordinates.clone();
                    tempCoordinates0.remove(0);
                    tempCoordinates1.remove(1);
                    tempCoordinates2.remove(2);
                    tempCoordinates3.remove(3);
                    tempCoordinates0.add(coordSet);
                    tempCoordinates1.add(coordSet);
                    tempCoordinates2.add(coordSet);
                    tempCoordinates3.add(coordSet);
                    double originArea = getAreaInSquareKm(coordinates);
                    double Area0 = getAreaInSquareKm(tempCoordinates0);
                    double Area1 = getAreaInSquareKm(tempCoordinates1);
                    double Area2 = getAreaInSquareKm(tempCoordinates2);
                    double Area3 = getAreaInSquareKm(tempCoordinates3);
                    System.out.println("########################################################################################");
                    System.out.println("OriginArea: "+ originArea);
                    System.out.println("Areas: "+ Area0);
                    System.out.println("Areas: "+ Area1);
                    System.out.println("Areas: "+ Area2);
                    System.out.println("Areas: "+ Area3);
                    if(Area0 >= originArea && Area0 >= Area1 && Area0 >= Area2 && Area0 >= Area3){
                        coordinates = (Vector<double[]>) tempCoordinates0.clone();
                    }else if(Area1 >= originArea && Area1 >= Area0 && Area1 >= Area2 && Area1 >= Area3){
                        coordinates = (Vector<double[]>) tempCoordinates1.clone();
                    }else if(Area2 >= originArea && Area2 >= Area1 && Area2 >= Area0 && Area2 >= Area3){
                        coordinates = (Vector<double[]>) tempCoordinates2.clone();
                    }else if(Area3 >= originArea && Area3 >= Area1 && Area3 >= Area2 && Area3 >= Area0){
                        coordinates = (Vector<double[]>) tempCoordinates3.clone();
                    }
                }

            }
            area = this.getAreaInSquareKm(coordinates);
        } catch (Exception e) {
            e.printStackTrace();
        }


        out.collect(new Tuple2<>(userName,area+""));


    }

    public double getAreaInSquareMeters(){
        return 0.0;
    }

    /**
     * calculates the area the Vector of coordinates defines
     * @param coordinates
     * @return
     */
    public double getAreaInSquareKm(Vector<double[]> coordinates){
        double area = 0.;
        double maxArea = 0.;
        double[] a = coordinates.get(0);
        double[] b = coordinates.get(1);
        double[] c = coordinates.get(2);
        double ab = this.haversineInKm(a[0],a[1],b[0],b[1]);
        double ac = this.haversineInKm(a[0],a[1],c[0],c[1]);
        double bc = this.haversineInKm(b[0],b[1],c[0],c[1]);
        if(coordinates.size() == 3){
            return this.round(this.heronForm(ab,ac,bc),decimalPlaces);
        }else{
            double[] d = coordinates.get(3);
            double ad = this.haversineInKm(a[0],a[1],d[0],d[1]);
            double bd = this.haversineInKm(b[0],b[1],d[0],d[1]);
            double cd = this.haversineInKm(c[0],c[1],d[0],d[1]);
            double abc = this.heronForm(ab,ac,bc);
            double abd = this.heronForm(ab,ad,bd);
            double acd = this.heronForm(ac,ad,cd);
            double bcd = this.heronForm(bc,bd,cd);
            boolean konvex = false;
            if(abc > abd && abc > acd && abc > bcd){
                if(abc*0.9 <= abd + acd + bcd && abd + acd + bcd <= abc*1.1){
                    return this.round(abc,decimalPlaces);
                }else{
                    konvex = true;
                }
            }else if(abd > abc && abd > acd && abd > bcd){
                if(abd*0.9 <= abc + acd + bcd && abd*1.1 >= abc + acd + bcd){
                    return this.round(abd,decimalPlaces);
                }else{
                    konvex = true;
                }
            }else if(acd > abc && acd > abd && acd > bcd){
                if(acd*0.9 <= abd + abc + bcd && acd*1.1 >= abd + abc + bcd){
                    return this.round(acd,decimalPlaces);
                }else{
                    konvex = true;
                }
            }else if(bcd > abc && bcd > abd && bcd > acd){
                if(bcd*0.9 <= abd + acd + abc && bcd*1.1 >= abd + acd + abc){
                    return this.round(bcd,decimalPlaces);
                }else{
                    konvex = true;
                }
            }else if(!konvex){
                return 0.66666;
            }
            if(konvex){
                if(ab<ac){
                    if(ac<ad){
                        // ad is the diagonal
                        return this.round(this.heronForm(ab,bd,ad) + this.heronForm(ac,cd,ad),decimalPlaces);
                    }else{
                        // ac is the diagonal
                        return this.round(this.heronForm(ab,bc,ac) + this.heronForm(ad,cd,ac),decimalPlaces);
                    }
                }else{
                    if(ab<ad){
                        // ad is the diagonal
                        return this.round(this.heronForm(ab,bd,ad) + this.heronForm(ac,cd,ad),decimalPlaces);
                    }else{
                        // ab is the diagonal
                        return this.round(this.heronForm(ac,bc,ab) + this.heronForm(ad,cd,ab),decimalPlaces);
                    }
                }
            }
        }
        return 0.;
    }

    /**
     * Calculates the area of a triangle using the heron formula
     * @param sideA
     * @param sideB
     * @param sideC
     * @return
     */
    public double heronForm(double sideA, double sideB, double sideC){
        double s = (sideA+sideB+sideC)/2;
        return Math.sqrt(s*(s-sideA)*(s-sideB)*(s-sideC));
    }


    /**
     * calculates the distance between two sets of coordinates using the haversine formula
     * @param lat1
     * @param long1
     * @param lat2
     * @param long2
     * @return
     */
    public double haversineInKm(double lat1, double long1, double lat2, double long2){
        double dlong = (long2 - long1) * deg2rad;
        double dlat = (lat2 - lat1) * deg2rad;
        double a = Math.pow(Math.sin(dlat / 2.), 2.) + Math.cos(lat1 * deg2rad) * Math.cos(lat2 * deg2rad) * Math.pow(Math.sin(dlong / 2.), 2.);
        double c = 2D * Math.atan2(Math.sqrt(a), Math.sqrt(1. - a));
        double d = equatorialEarthRadius * c;

        return d;
    }

    /**
     * Rounds the given double to the given decimal places
     * @param value
     * @param places
     * @return
     */
    private double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }
}

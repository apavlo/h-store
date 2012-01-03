/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.benchmark.seats.util;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.catalog.Table;
import org.voltdb.utils.Pair;

import au.com.bytecode.opencsv.CSVWriter;
import edu.brown.benchmark.seats.SEATSConstants;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.TableDataIterable;

/**
 * Based on code found here:
 * http://www.zipcodeworld.com/samples/distance.java.html
 */
public abstract class DistanceUtil {

    /**
     * Calculate the distance between two points
     * @param lat0
     * @param lon0
     * @param lat1
     * @param lon1
     * @return
     */
    public static double distance(double lat0, double lon0, double lat1, double lon1) {
        double theta = lon0 - lon1;
        double dist = Math.sin(deg2rad(lat0)) * Math.sin(deg2rad(lat1)) + Math.cos(deg2rad(lat0)) * Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(theta));
        dist = Math.acos(dist);
        dist = rad2deg(dist);
        return (dist * 60 * 1.1515);
    }
    
    /**
     * Pair<Latitude, Longitude>
     * @param loc0
     * @param loc1
     * @return
     */
    public static double distance(Pair<Double, Double> loc0, Pair<Double, Double> loc1) {
        return (DistanceUtil.distance(loc0.getFirst(), loc0.getSecond(), loc1.getFirst(), loc1.getSecond()));
    }

    private static double deg2rad(double deg) {
        return (deg * Math.PI / 180.0);
    }

    private static double rad2deg(double rad) {
        return (rad * 180.0 / Math.PI);
    }
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG);
        
        File data_dir = new File(args.getOptParam(0));
        assert(data_dir.exists());
        
        Table catalog_tbl = args.catalog_db.getTables().get(SEATSConstants.TABLENAME_AIRPORT);
        assert(catalog_tbl != null);
        File f = new File(data_dir.getAbsolutePath() + File.separator + "table." + catalog_tbl.getName().toLowerCase() + ".csv");
        if (f.exists() == false) f = new File(f.getAbsolutePath() + ".gz");
        TableDataIterable iterable = new TableDataIterable(catalog_tbl, f, true, true);
        Map<Long, Pair<Double, Double>> locations = new HashMap<Long, Pair<Double,Double>>();
        for (Object row[] : iterable) {
            Long code = (Long)row[0];
            Double longitude = (Double)row[6];
            Double latitude = (Double)row[7];
            locations.put(code, Pair.of(latitude, longitude));
        } // FOR

        File output = new File("table.airport_distance.csv");
        CSVWriter writer = new CSVWriter(new FileWriter(output));
        
        // HEADER
        writer.writeNext(new String[]{ "D_AP_ID0", "D_AP_ID1", "D_DISTANCE" });

        // DATA
        long ctr = 0;
        String row[] = new String[3];
        for (Long id0 : locations.keySet()) {
            Pair<Double, Double> outer = locations.get(id0);
            row[0] = id0.toString();
            
            for (Long id1 : locations.keySet()) {
                if (id0.equals(id1)) continue;
                Pair<Double, Double> inner = locations.get(id1);
                Double distance = distance(outer, inner);
            
                row[1] = id1.toString();
                row[2] = distance.toString();
                writer.writeNext(row);
            } // FOR
            if (++ctr % 100 == 0) System.err.println(String.format("Processed %d / %d", ctr, locations.size()));
        } // FOR
    }
    
}

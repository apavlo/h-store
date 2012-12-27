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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;
import edu.brown.benchmark.seats.SEATSConstants;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;
import edu.brown.utils.StringUtil;

public class GenerateHistograms {
    private static final Logger LOG = Logger.getLogger(GenerateHistograms.class);
    
    final ObjectHistogram<String> flights_per_airline = new ObjectHistogram<String>();
    final ObjectHistogram<String> flights_per_time = new ObjectHistogram<String>();
    final Map<String, ObjectHistogram<String>> flights_per_airport = new TreeMap<String, ObjectHistogram<String>>();
    
    public GenerateHistograms() {
        // Nothing...
    }
    

    public static GenerateHistograms generate(File input) throws Exception {
        GenerateHistograms gh = new GenerateHistograms();
        
        final ListOrderedMap<String, Integer> columns_xref = new ListOrderedMap<String, Integer>();
        CSVReader reader = new CSVReader(FileUtil.getReader(input));
        String row[] = null;
        boolean first = true;
        while ((row = reader.readNext()) != null) {
            if (first) {
                for (int i = 0; i < row.length; i++) {
                    columns_xref.put(row[i].toUpperCase(), i);
                } // FOR
                first = false;
                continue;
            }
            if (row[0].equalsIgnoreCase("Year")) continue;
            
            String airline_code = row[columns_xref.get("UNIQUECARRIER")];
            String depart_airport_code = row[columns_xref.get("ORIGIN")];
            String arrival_airport_code = row[columns_xref.get("DEST")];
            String depart_time = row[columns_xref.get("CRSDEPTIME")];
            
            // Flights Per Airline
            gh.flights_per_airline.put(airline_code);
            
            // Flights Per Time
            // Convert the time into "HH:MM" and round to the nearest 15 minutes
            int hour = Integer.parseInt(depart_time.substring(0, 2));
            int minute = Integer.parseInt(depart_time.substring(2, 4));
            minute = (minute / 15) * 15;
            gh.flights_per_time.put(String.format("%02d:%02d", hour, minute));
            
            // Flights Per Airport
            // DepartAirport -> Histogram<ArrivalAirport>
            ObjectHistogram<String> h = gh.flights_per_airport.get(depart_airport_code);
            if (h == null) {
                h = new ObjectHistogram<String>();
                gh.flights_per_airport.put(depart_airport_code, h);
            }
            h.put(arrival_airport_code);
        } // WHILE
        reader.close();
        return (gh);
    }
    
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        
        File csv_path = new File(args.getOptParam(0));
        File output_path = new File(args.getOptParam(1));
        
        GenerateHistograms gh = GenerateHistograms.generate(csv_path);
        
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Airport Codes", gh.flights_per_airport.size());
        m.put("Airlines", gh.flights_per_airline.getValueCount());
        m.put("Departure Times", gh.flights_per_time.getValueCount());
        LOG.info(StringUtil.formatMaps(m));
        
        System.err.println(StringUtil.join("\n", gh.flights_per_airport.keySet()));
        
        Map<String, Histogram<?>> histograms = new HashMap<String, Histogram<?>>();
        histograms.put(SEATSConstants.HISTOGRAM_FLIGHTS_PER_DEPART_TIMES, gh.flights_per_time);
        // histograms.put(SEATSConstants.HISTOGRAM_FLIGHTS_PER_AIRLINE, gh.flights_per_airline);
        histograms.put(SEATSConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT,
                       SEATSHistogramUtil.collapseAirportFlights(gh.flights_per_airport));
        
        for (Entry<String, Histogram<?>> e : histograms.entrySet()) {
            File output_file = new File(output_path.getAbsolutePath() + "/" + e.getKey() + ".histogram");
            LOG.info(String.format("Writing out %s data to '%s' [samples=%d, values=%d]",
                     e.getKey(), output_file, e.getValue().getSampleCount(), e.getValue().getValueCount()));
            e.getValue().save(output_file);
        } // FOR
    }
}

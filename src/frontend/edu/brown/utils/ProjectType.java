/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
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
package edu.brown.utils;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum ProjectType {

    TPCC("TPC-C", "org.voltdb.benchmark.tpcc"),
    TPCE("TPC-E", "edu.brown.benchmark.tpce"),
    TM1("TM1", "edu.brown.benchmark.tm1"),
    SIMPLE("Simple", null),
    SEATS("SEATS", "edu.brown.benchmark.seats"),
    MARKOV("Markov", "edu.brown.benchmark.markov"),
    BINGO("Bingo", "org.voltdb.benchmark.bingo"),
    AUCTIONMARK("AuctionMark", "edu.brown.benchmark.auctionmark"),
    LOCALITY("Locality", "edu.brown.benchmark.locality"),
    MAPREDUCE("MapReduce", "edu.brown.benchmark.mapreduce"),
    WIKIPEDIA("Wikipedia", "edu.brown.benchmark.wikipedia"),
    EXAMPLE("Example", "edu.brown.benchmark.example"),
    TEST("Test", null),
    ;


    private final String package_name;
    private final String benchmark_name;

    private ProjectType(String benchmark_name, String package_name) {
        this.benchmark_name = benchmark_name;
        this.package_name = package_name;
    }

    public String getBenchmarkName() {
        return (this.benchmark_name);
    }

    public String getBenchmarkPrefix() {
        return (this.benchmark_name.replace("-", ""));
    }

    /**
     * Returns the package name for where this We need this because we need to
     * be able to dynamically reference various things from the 'src/frontend'
     * directory before we compile the 'tests/frontend' directory
     * 
     * @return
     */
    public String getPackageName() {
        return (this.package_name);
    }

    protected static final Map<Integer, ProjectType> idx_lookup = new HashMap<Integer, ProjectType>();
    protected static final Map<String, ProjectType> name_lookup = new HashMap<String, ProjectType>();
    static {
        for (ProjectType vt : EnumSet.allOf(ProjectType.class)) {
            ProjectType.idx_lookup.put(vt.ordinal(), vt);
            ProjectType.name_lookup.put(vt.name().toLowerCase().intern(), vt);
        } // FOR
    }

    public static ProjectType get(Integer idx) {
        return (ProjectType.idx_lookup.get(idx));
    }

    public static ProjectType get(String name) {
        return (ProjectType.name_lookup.get(name.toLowerCase().intern()));
    }
}

/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original By: VoltDB Inc.                                               *
 *  Ported By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)  *
 *                                                                         *
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

package edu.brown.benchmark.bikerstream;

import java.util.Random;
import edu.brown.utils.MathUtil;
import java.util.LinkedList;
import java.util.Scanner;
import java.io.File;


  /**
   * Class to generate readings from bikes. Idea is that
   * this class generates readings like might be generated
   * from a bike GPS unit.

   * I used the clientId as the bikeid (so each client would
   * have its own bikeid) and the lat/lon are completely made
   * up - probably in antartica or something.
   */
public class BikeReadingGenerator {

    private LinkedList<Reading> ll = new LinkedList();

    public BikeReadingGenerator(int id) throws Exception {
        makePoints(Integer.toString(id));
    }

    public BikeReadingGenerator(String filename) throws Exception {
        makePoints(filename);
    }

    public class Reading {
        public double lat;
        public double lon;

        public Reading(double lat, double lon) {
            this.lat = lat;
            this.lon = lon;
        }
    }

    /*
     * Read a File containing the GPS events for a ride and store
     * them in an internall Linked List. Function has no return.
     * Only used for it's side-effects used to accumulate the gps events.
     */
    public void makePoints(String filename) throws Exception {

        File    f = new File(filename);
        Scanner s = new Scanner(f);

        try {

            double lat_t;
            double lon_t;
            double alt_t;

            while (s.hasNext()){

                lat_t = s.nextDouble();
                lon_t = s.nextDouble();
                alt_t = s.nextDouble();

                ll.add(new Reading(lat_t, lon_t));
            }
        }
        catch (Exception e) {
            System.out.println("File Not Found" + e);
        }
        finally {
           s.close();
        }
    }

    public Reading getPoint() {
        if(hasPoints()) {
            Reading ret = ll.pop();
            return ret;
        }

        return null;
    }

    public boolean hasPoints() {
        int lsize = ll.size();
        if (lsize > 0)
            return true;
        return false;
    }



    /**
     * Receives/generates a simulated voting call
     *
     * @return Call details (calling number and contestant to whom the vote is given)
     */
    public Reading pedal()
    {
        return getPoint();
    }

}

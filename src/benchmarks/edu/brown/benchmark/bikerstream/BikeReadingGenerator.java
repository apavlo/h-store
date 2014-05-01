/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original By: VoltDB Inc.											   *
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

public class BikeReadingGenerator {
	
    private int bikeId;
    private final Random rand = new Random();
    private long lat;	
    private long lon;

	public static class Reading {
	    public final int bikeId;
        public final long lat;
        public final long lon;
		
        protected Reading(int bikeId, long lat, long lon) {
            this.bikeId= bikeId;
            this.lat= lat;
            this.lon= lon;
        }
    }
	
	public BikeReadingGenerator(int clientId) {
      // try using clientid as the bikeid
      // requirement (for now) is different clients need to
      // use different bikeids
	    this.bikeId = clientId;

      // start lat/lon at some position related to the
      // bikeId, then increment
      // totally invalid - but can work for debugging
      this.lat = bikeId*1000;
      this.lon = bikeId*1000;
    }
	
	/**
     * Receives/generates a simulated voting call
     * @return Call details (calling number and contestant to whom the vote is given)
     */
    public Reading pedal()
    {
        // For the purpose of a benchmark, issue random voting activity
        // (including invalid votes to demonstrate transaction validationg in the database)
        // TODO will want to add randomness for this same
        // reason for bikerstream
		
        //  introduce an invalid contestant every 100 call or so to simulate fraud
        //  and invalid entries (something the transaction validates against)
        //if (rand.nextInt(100) == 0) {
        //    contestantNumber = 999;
        //}
		
        return new Reading(bikeId, lat++, lon++);
    }

}

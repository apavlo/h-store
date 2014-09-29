/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Coded By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)   *
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

import edu.brown.benchmark.bikerstream.procedures.*;
import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

public class BikerStreamProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = BikerStreamClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = BikerStreamLoader.class;

    // a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[]{
            AcceptDiscount.class,
            Bikes.class,
            BikeStatus.class,
            CheckinBike.class,
            CheckoutBike.class,
            DetectAnomalies.class,
            FindUser.class,
            GetAnomalies.class,
            GetBikeStatus.class,
            GetBikeStatusByBikeId.class,
            GetNearDiscounts.class,
            GetRecentRiderArea.class,
            GetRecentRiderSummary.class,
            GetStationStatus.class,
            Initialize.class,
            LastNBikeStatusTrigger.class,
            LastNRiderSpeedsTrigger.class,
            LogRiderTrip.class,
            ProcessBikeStatus.class,
            RideBike.class,
            SignUp.class,
            SignUpName.class,
            SignUpRand.class,
            Stations.class,
            StationStatus.class,
            TestProcedure.class,
            UpdateNearByDiscounts.class,
            UpdateNearByStations.class,
            UpdateRiderLocations.class,
            UserLocations.class,
            Users.class
    };

    // a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
            {"bikestatus", "user_id" },
            {"lastNBikestatus", "user_id" },
            {"s1", "user_id"},
            {"riderSpeeds", "user_id"},
            {"lastNRiderSpeeds", "user_id"},
            {"s3", "user_id"}
    };

    public BikerStreamProjectBuilder() {
        super("bikerstream", BikerStreamProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}





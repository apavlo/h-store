/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

/* Initialize - nothing to do */

package edu.brown.benchmark.bikerstream.procedures;

import edu.brown.benchmark.bikerstream.BikerStreamConstants;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltType;

@ProcInfo (
singlePartition = false
)
public class SignUp extends VoltProcedure
{

    public final SQLStmt insertRider = new SQLStmt(
        "INSERT INTO riders (rider_id, f_name, l_name) VALUES (?,?,?);"
    );

    public final SQLStmt insertCard = new SQLStmt(
        "INSERT INTO cards (rider_id, bank, name, num, exp, sec_code) VALUES (?,?,?,?,?,?);"
    );

    public long run(int rider_id) {


        // Get a random number coresponding to the length of the name arrays
        int rand1 = (int) (Math.random() * (float) BikerStreamConstants.FIRSTNAMES.length);
        int rand2 = (int) (Math.random() * (float) BikerStreamConstants.LASTNAMES.length);

        // Get a random name
        String first = BikerStreamConstants.FIRSTNAMES[rand1];
        String last  = BikerStreamConstants.LASTNAMES[rand2];

        try {
            voltQueueSQL(insertRider, rider_id, first, last);
            voltExecuteSQL();
        } catch (Exception e) {
            throw new RuntimeException("Failure to load rider " + rider_id + " into the DB");
        }

        try {
            voltQueueSQL(insertCard,
                         rider_id,               // The id of the rider
                         "BOA",                  // The bank name
                         first + " " + last,     // Cardholdername
                         "11112222333344445555", // Carn number
                         "Nov/2020",             // Expiration date
                         123);                   // Security code
            voltExecuteSQL();
        } catch (Exception e) {
            return BikerStreamConstants.INSERT_CARD_FAILED;
        }

        return BikerStreamConstants.INSERT_RIDER_SUCCESS;

    }

}

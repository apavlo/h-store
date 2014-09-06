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
import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import java.util.Random;


@ProcInfo (
singlePartition = false
)
public class SignUpName extends VoltProcedure
{
    private static final Logger Log = Logger.getLogger(SignUpName.class);

    public final SQLStmt insertRider = new SQLStmt(
        "INSERT INTO users (user_id, user_name, credit_card, membership_status, membership_expiration_date) " +
        "VALUES (?,?,?,?,?)"
    );

    public final SQLStmt checkID = new SQLStmt(
            "SELECT count(*) FROM users WHERE user_id = ?"
    );

    public VoltTable run(String first, String last) {

        Random gen = new Random();
        VoltTable result;
        VoltTable vt;
        String full = first + " " + last;
        long user_id = 0;

        try {
            do {
                user_id = (long) gen.nextInt(BikerStreamConstants.MAX_ID);
                voltQueueSQL(checkID, user_id);
                result = voltExecuteSQL()[0];
            } while (result.asScalarLong() > 0);

            voltQueueSQL(insertRider, user_id, full, "0000000000111112222233333", 1, new TimestampType());
            voltExecuteSQL(true);

        } catch (Exception e) {
            vt = new VoltTable(new VoltTable.ColumnInfo("", VoltType.INTEGER));
            vt.addRow(BikerStreamConstants.FAILED_SIGNUP);
            return vt;
            //return BikerStreamConstants.FAILED_SIGNUP;
        }

        vt = new VoltTable(new VoltTable.ColumnInfo("USER_ID", VoltType.INTEGER));
        vt.addRow(user_id);
        return vt;

    }

}

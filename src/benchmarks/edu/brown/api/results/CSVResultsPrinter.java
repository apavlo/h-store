/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package edu.brown.api.results;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.api.BenchmarkController;

public class CSVResultsPrinter implements BenchmarkController.BenchmarkInterest {

    public static final ColumnInfo COLUMNS[] = {
        new ColumnInfo("INTERVAL", VoltType.INTEGER),
        new ColumnInfo("TRANSACTIONS", VoltType.INTEGER),
        new ColumnInfo("THROUGHPUT", VoltType.DECIMAL),
        new ColumnInfo("LATENCY", VoltType.DECIMAL),
        new ColumnInfo("CREATED", VoltType.TIMESTAMP),
    };

    private final VoltTable results;
    private final File outputPath;
    
    public CSVResultsPrinter(File outputPath) {
        this.results = new VoltTable(COLUMNS);
        this.outputPath = outputPath;
    }
    
    @Override
    public String formatFinalResults(BenchmarkResults br) {
        try {
            FileWriter writer = new FileWriter(this.outputPath);
            VoltTableUtil.csv(writer, this.results, true);
            writer.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        String msg = "Wrote CSV results to '" + this.outputPath.getAbsolutePath() + "'";
        return (msg);
    }
    
    @Override
    public void benchmarkHasUpdated(BenchmarkResults br) {
        Pair<Long, Long> p = br.computeTotalAndDelta();
        assert(p != null);
        
        long txnDelta = p.getSecond();
        double tps = (txnDelta / (double)(br.getIntervalDuration()) * 1000.0);
        
        this.results.addRow(
            br.getCompletedIntervalCount(),
            txnDelta,
            tps,
            null,
            new TimestampType()
        );
    }
}

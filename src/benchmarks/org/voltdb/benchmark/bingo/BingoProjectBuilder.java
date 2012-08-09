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
package org.voltdb.benchmark.bingo;

import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.bingo.procedures.CreateTournament;
import org.voltdb.benchmark.bingo.procedures.DeleteTournament;
import org.voltdb.benchmark.bingo.procedures.GetAvgPot;
import org.voltdb.benchmark.bingo.procedures.PlayRound;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

public class BingoProjectBuilder extends AbstractProjectBuilder {
    
    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends BenchmarkComponent> m_clientClass = BingoClient.class;
    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends BenchmarkComponent> m_loaderClass = null;
    
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        CreateTournament.class,
        PlayRound.class,
        DeleteTournament.class,
        GetAvgPot.class
    };
    
    public static final String PARTITIONING[][] = 
        new String[][] {
            {"T", "T_ID"},
            {"B", "T_ID"},
            {"R", "T_ID"},
        };
    
    
    public BingoProjectBuilder() {
        super("bingo", BingoProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

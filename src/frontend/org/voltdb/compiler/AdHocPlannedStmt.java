/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.compiler;

import edu.brown.hstore.dtxn.LocalTransaction;

public class AdHocPlannedStmt extends AsyncCompilerResult {
    private static final long serialVersionUID = -8627490621430290801L;

    public String aggregatorFragment;
    public String collectorFragment;
    public String sql;
    public boolean isReplicatedTableDML;
    
    public AdHocPlannedStmt(LocalTransaction ts) {
        super(ts);
    }

    @Override
    public String toString() {
        String retval = super.toString();
        retval += "sql: " + ((sql != null) ? sql : "null");
        return retval;
    }
}

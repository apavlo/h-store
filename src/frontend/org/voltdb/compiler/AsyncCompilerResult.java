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

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.Map;

import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.StringUtil;

public class AsyncCompilerResult implements Serializable {
    private static final long serialVersionUID = -1538141431615585812L;

    public LocalTransaction ts;
    public long clientHandle = -1;
    public String errorMsg = null;
    public int expectedCatalogVersion = -1;
    
    public AsyncCompilerResult(LocalTransaction ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        Class<?> confClass = this.getClass();
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        for (Field f : confClass.getFields()) {
            Object obj = null;
            try {
                obj = f.get(this);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            m.put(f.getName().toUpperCase(), obj);
        } // FOR
        return (StringUtil.formatMaps(m));
    }
}

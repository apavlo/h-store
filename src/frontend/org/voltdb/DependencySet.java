/* This file is part of VoltDB.
 * Copyright (C) 2008-2009 VoltDB L.L.C.
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

package org.voltdb;

import java.util.Map;

import org.apache.commons.collections15.map.ListOrderedMap;

import edu.brown.utils.StringUtil;

public class DependencySet {

    public final int[] depIds;
    public final VoltTable[] dependencies;

    public DependencySet(int depId, VoltTable dependency) {
        this(new int[]{depId}, new VoltTable[]{dependency});
    }
    
    public DependencySet(int[] depIds, VoltTable[] dependencies) {
        assert(depIds != null);
        assert(dependencies != null);
        assert(depIds.length == dependencies.length);

        this.depIds = depIds;
        this.dependencies = dependencies;
    }

    public int size() {
        return depIds.length;
    }
    
    @Override
    public String toString() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (int i = 0; i < this.dependencies.length; i++) {
            m.put(Integer.toString(this.depIds[i]), this.dependencies[i].toString());
        } // FOR
        return (StringUtil.formatMaps(m));
        
    }
}

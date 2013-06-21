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

package org.voltdb.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Table Partitioning Method
 */
public enum PartitionMethodType {
    INVALID     (0),
    HASH        (1),
    RANGE       (2),
    MAP         (3),
    REPLICATION (4),
    NONE        (5),
    RANDOM      (6);

    PartitionMethodType(int val) {
        assert (this.ordinal() == val) :
            "Enum element " + this.name() +
            " in position " + this.ordinal() +
            " instead of position " + val;
    }

    public int getValue() {
        return this.ordinal();
    }

    private static final Map<String, PartitionMethodType> name_lookup = new HashMap<String, PartitionMethodType>();
    static {
        for (PartitionMethodType vt : EnumSet.allOf(PartitionMethodType.class)) {
            PartitionMethodType.name_lookup.put(vt.name().toLowerCase(), vt);
        }
    }

    public static PartitionMethodType get(Integer idx) {
        PartitionMethodType values[] = PartitionMethodType.values();
        if (idx < 0 || idx >= values.length) {
            return (PartitionMethodType.INVALID);
        }
        return (values[idx]);
    }

    public static PartitionMethodType get(String name) {
        PartitionMethodType ret = PartitionMethodType.name_lookup.get(name.toLowerCase());
        return (ret == null ? PartitionMethodType.INVALID : ret);
    }
}

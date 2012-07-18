package org.voltdb.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Type that specifies what kind of conflict exists between transactions
 */
public enum ConflictType {
    INVALID     (0), // For Parsing...
    READ_WRITE  (1),
    WRITE_READ  (2),
    WRITE_WRITE (3);

    ConflictType(int val) {
        assert (this.ordinal() == val) :
            "Enum element " + this.name() +
            " in position " + this.ordinal() +
            " instead of position " + val;
    }

    public int getValue() {
        return this.ordinal();
    }

    protected static final Map<Integer, ConflictType> idx_lookup = new HashMap<Integer, ConflictType>();
    protected static final Map<String, ConflictType> name_lookup = new HashMap<String, ConflictType>();
    static {
        for (ConflictType vt : EnumSet.allOf(ConflictType.class)) {
            ConflictType.idx_lookup.put(vt.ordinal(), vt);
            ConflictType.name_lookup.put(vt.name().toLowerCase().intern(), vt);
        }
    }

    public static Map<Integer, ConflictType> getIndexMap() {
        return idx_lookup;
    }

    public static Map<String, ConflictType> getNameMap() {
        return name_lookup;
    }

    public static ConflictType get(Integer idx) {
        ConflictType ret = ConflictType.idx_lookup.get(idx);
        return (ret == null ? ConflictType.INVALID : ret);
    }

    public static ConflictType get(String name) {
        ConflictType ret = ConflictType.name_lookup.get(name.toLowerCase().intern());
        return (ret == null ? ConflictType.INVALID : ret);
    }

}

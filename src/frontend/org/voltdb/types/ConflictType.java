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

    private static final Map<String, ConflictType> name_lookup = new HashMap<String, ConflictType>();
    static {
        for (ConflictType vt : EnumSet.allOf(ConflictType.class)) {
            ConflictType.name_lookup.put(vt.name().toLowerCase(), vt);
        }
    }

    public static ConflictType get(Integer idx) {
        ConflictType values[] = ConflictType.values();
        if (idx < 0 || idx >= values.length) {
            return (ConflictType.INVALID);
        }
        return (values[idx]);
    }

    public static ConflictType get(String name) {
        ConflictType ret = ConflictType.name_lookup.get(name.toLowerCase());
        return (ret == null ? ConflictType.INVALID : ret);
    }

}

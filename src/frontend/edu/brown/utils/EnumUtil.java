package edu.brown.utils;

/**
 * @author pavlo
 */
public abstract class EnumUtil {

    /**
     * Return the Enum that matches the given name
     * @param <E>
     * @param members
     * @param name
     * @return
     */
    public static <E extends Enum<?>> E get(E members[], String name) {
        for (E e : members) {
            if (e.name().equals(name)) return (e);
        } // FOR
        return (null);
    }

    /**
     * Return the Enum at the given index
     * @param <E>
     * @param members
     * @param idx
     * @return
     */
    public static <E extends Enum<?>> E get(E members[], int idx) {
        for (E e : members) {
            if (e.ordinal() == idx) return (e);
        } // FOR
        return (null);
    }
    
}

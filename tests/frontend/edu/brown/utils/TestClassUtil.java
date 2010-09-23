package edu.brown.utils;

import java.io.Serializable;
import java.util.*;

import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;

import edu.brown.catalog.special.MultiColumn;

import junit.framework.TestCase;

public class TestClassUtil extends TestCase {
    
    private final Class<?> target_class = ArrayList.class;
    

    /**
     * testGetSuperClasses
     */
    public void testGetSuperClasses() {
        Class<?> expected[] = {
            target_class,
            AbstractList.class,
            AbstractCollection.class,
            Object.class,
        };
        List<Class<?>> results = ClassUtil.getSuperClasses(target_class);
        // System.err.println(target_class + " => " + results);
        assert(!results.isEmpty());
        assertEquals(expected.length, results.size());
        
        for (Class<?> e : expected) {
            assert(results.contains(e));
        } // FOR
    }
    

    /**
     * testGetSuperClassesCatalogType
     */
    public void testGetSuperClassesCatalogType() {
        Class<?> expected[] = {
            MultiColumn.class,
            Column.class,
            CatalogType.class,
            Object.class,
        };
        List<Class<?>> results = ClassUtil.getSuperClasses(MultiColumn.class);
//        System.err.println(target_class + " => " + results);
        assert(!results.isEmpty());
        assertEquals(expected.length, results.size());
        
        for (Class<?> e : expected) {
            assert(results.contains(e));
        } // FOR
    }
    
    /**
     * GetInterfaces
     */
    public void testGetInterfaces() {
        Class<?> expected[] = {
            Serializable.class,
            Cloneable.class,
            Iterable.class,
            Collection.class,
            List.class,
            RandomAccess.class,
        };
        Set<Class<?>> results = ClassUtil.getInterfaces(target_class);
        // System.err.println(target_class + " => " + results);
        assert(!results.isEmpty());
        assertEquals(expected.length, results.size());
        
        for (Class<?> e : expected) {
            assert(results.contains(e));
        } // FOR
    }
}

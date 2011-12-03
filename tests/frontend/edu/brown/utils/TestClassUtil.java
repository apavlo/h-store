package edu.brown.utils;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.*;

import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;

import edu.brown.BaseTestCase;
import edu.brown.catalog.special.MultiColumn;

public class TestClassUtil extends BaseTestCase {
    
    private final Class<?> target_class = ArrayList.class;
    
    public static class MockObject1 {
        public MockObject1(MockObject1 x) {
            
        }
    }
    public static class MockObject2 {
        public MockObject2(MockObject2 x) {
            
        }
    }
    
    
    /**
     * testGetConstructor
     */
    public void testGetConstructor() throws Exception {
        Class<?> targets[] = {
            MockObject1.class,
            MockObject2.class,
        };
        Class<?> params[] = {
            MockObject1.class
        };
        
        for (Class<?> targetClass : targets) {
            Constructor<?> c = ClassUtil.getConstructor(targetClass, params);
            assertNotNull(c);
        } // FOR
    }
    

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
        Collection<Class<?>> results = ClassUtil.getInterfaces(target_class);
        // System.err.println(target_class + " => " + results);
        assert(!results.isEmpty());
        assertEquals(expected.length, results.size());
        
        for (Class<?> e : expected) {
            assert(results.contains(e));
        } // FOR
    }
}

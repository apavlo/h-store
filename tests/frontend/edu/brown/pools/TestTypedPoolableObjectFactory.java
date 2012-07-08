package edu.brown.pools;

import org.apache.commons.pool.impl.StackObjectPool;

import edu.brown.pools.TypedPoolableObjectFactory;

import junit.framework.TestCase;

public class TestTypedPoolableObjectFactory extends TestCase {

    public static class MockObject implements Poolable {
        boolean finished = false;
        @Override
        public void finish() {
            this.finished = true;
        }
        @Override
        public boolean isInitialized() {
            return (this.finished == false);
        }
    }
    
    public static class MockObjectWithArgs extends MockObject {
        final Integer value0;
        final String value1;
        
        public MockObjectWithArgs(Integer value0, String value1) {
            this.value0 = value0;
            this.value1 = value1;
        }
    }
    
    /**
     * testMakeFactoryNoArguments
     */
    public void testMakeFactoryNoArguments() throws Exception {
        TypedPoolableObjectFactory<MockObject> factory = TypedPoolableObjectFactory.makeFactory(MockObject.class, true);
        assertNotNull(factory);
        
        StackObjectPool pool = new StackObjectPool(factory);
        
        MockObject obj = (MockObject)pool.borrowObject();
        assertNotNull(obj);
        assertTrue(obj.isInitialized());
    }
    
    /**
     * testMakeFactoryArguments
     */
    public void testMakeFactoryArguments() throws Exception {
        Integer expected0 = 12345;
        String expected1 = "ABC";
        TypedPoolableObjectFactory<MockObjectWithArgs> factory = TypedPoolableObjectFactory.makeFactory(MockObjectWithArgs.class, true, expected0, expected1);
        assertNotNull(factory);
        
        StackObjectPool pool = new StackObjectPool(factory);
        
        MockObjectWithArgs obj = (MockObjectWithArgs)pool.borrowObject();
        assertNotNull(obj);
        assertTrue(obj.isInitialized());
        assertEquals(expected0, obj.value0);
        assertEquals(expected1, obj.value1);
    }
    
}

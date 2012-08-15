/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.pools;

import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.BasePoolableObjectFactory;

import edu.brown.utils.ClassUtil;

/**
 * @author pavlo
 * @param <T>
 */
public abstract class TypedPoolableObjectFactory<T extends Poolable> extends BasePoolableObjectFactory {

    private boolean enable_counting;
    private final AtomicInteger created = new AtomicInteger(0);
    private final AtomicInteger passivated = new AtomicInteger(0);
    private final AtomicInteger destroyed = new AtomicInteger(0);

    public TypedPoolableObjectFactory(boolean enable_counting) {
        this.enable_counting = enable_counting;
    }

    public abstract T makeObjectImpl() throws Exception;

    public void setEnableCounting(boolean enable) {
        this.enable_counting = enable;
    }
    public boolean isCountingEnabled() {
        return (this.enable_counting);
    }
    
    @Override
    public final T makeObject() throws Exception {
        T obj = this.makeObjectImpl();
        if (this.enable_counting)
            this.created.getAndIncrement();
        return obj;
    }

    @Override
    public final void passivateObject(Object obj) throws Exception {
        Poolable poolable = (Poolable) obj;
        // There might be a race condition here... but maybe not...
        // if (poolable.isInitialized()) {
            poolable.finish();
        // }
        if (this.enable_counting)
            this.passivated.getAndIncrement();
    }

    @Override
    public final void destroyObject(Object obj) throws Exception {
        if (this.enable_counting)
            this.destroyed.getAndIncrement();
    }

    public int getCreatedCount() {
        return (this.created.get());
    }

    public int getPassivatedCount() {
        return (this.passivated.get());
    }

    public int getDestroyedCount() {
        return (this.destroyed.get());
    }
    
    @Override
    public String toString() {
        String ret = super.toString();
        if (this.enable_counting) {
            ret += String.format("[created:%d / passivated:%s / destroyed: %d]",
                                 this.created.get(),
                                 this.passivated.get(),
                                 this.destroyed.get());
        }
        return (ret);
    }

    /**
     * @param <X>
     * @param clazz
     * @param enable_tracking
     * @param args
     * @return
     */
    public static <X extends Poolable> TypedPoolableObjectFactory<X> makeFactory(final Class<X> clazz,
                                                                                   final boolean enable_tracking,
                                                                                   final Object... args) {
        Class<?> argsClazz[] = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            assert (args[i] != null) : "[" + i + "]";
            argsClazz[i] = args[i].getClass();
        } // FOR
        final Constructor<X> constructor = ClassUtil.getConstructor(clazz, argsClazz);
        return new TypedPoolableObjectFactory<X>(enable_tracking) {
            @Override
            public X makeObjectImpl() throws Exception {
                return (constructor.newInstance(args));
            }
        };
    }
}

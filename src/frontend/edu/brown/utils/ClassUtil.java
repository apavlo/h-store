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
package edu.brown.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.commons.lang.ClassUtils;
import org.apache.log4j.Logger;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * @author pavlo
 */
public abstract class ClassUtil {
    private static final Logger LOG = Logger.getLogger(ClassUtil.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private static final Map<Class<?>, List<Class<?>>> CACHE_getSuperClasses = new HashMap<Class<?>, List<Class<?>>>();
    private static final Map<Class<?>, Set<Class<?>>> CACHE_getInterfaceClasses = new HashMap<Class<?>, Set<Class<?>>>();

    /**
     * @param clazz
     * @return
     */
    public static <T> Field[] getFieldsByType(Class<?> clazz, Class<? extends T> fieldType) {
        List<Field> fields = new ArrayList<Field>();
        for (Field f : clazz.getDeclaredFields()) {
            int modifiers = f.getModifiers();
            if (Modifier.isTransient(modifiers) == false &&
                Modifier.isPublic(modifiers) == true &&
                Modifier.isStatic(modifiers) == false &&
                ClassUtil.getSuperClasses(f.getType()).contains(fieldType)) {
                
                fields.add(f);
            }
        } // FOR
        return (fields.toArray(new Field[fields.size()]));
    }
    
    /**
     * Returns true if asserts are enabled. This assumes that
     * we're always using the default system ClassLoader
     */
    public static boolean isAssertsEnabled() {
        boolean ret = false;
        try {
            assert(false);
        } catch (AssertionError ex) {
            ret = true;
        }
        return (ret);
    }
    
    /**
     * Convenience method to get the name of the method that invoked this method
     * This is slow and should not be used for anything other than debugging
     * @return
     */
    public static String getCurrentMethodName() {
        StackTraceElement stack[] = Thread.currentThread().getStackTrace();
        assert(stack[2] != null);
        return String.format("%s.%s", stack[2].getClassName(), stack[2].getMethodName());
    }
    
    /**
     * Return the stack trace for the location that calls this method.
     * @return
     */
    public static String[] getStackTrace() {
        String ret[] = null;
        try {
            throw new Exception();
        } catch (Exception ex) {
            StackTraceElement stack[] = ex.getStackTrace();
            ret = new String[stack.length-1];
            for (int i = 1; i < stack.length; i++) {
                ret[i-1] = stack[i].toString();
            } // FOR
        }
        return (ret);
    }
    
    /**
     * Check if the given object is an array (primitve or native).
     * http://www.java2s.com/Code/Java/Reflection/Checkifthegivenobjectisanarrayprimitveornative.htm
     * 
     * @param obj
     *            Object to test.
     * @return True of the object is an array.
     */
    public static boolean isArray(final Object obj) {
        return (obj != null ? obj.getClass().isArray() : false);
    }

    public static boolean[] isArray(final Object objs[]) {
        boolean is_array[] = new boolean[objs.length];
        for (int i = 0; i < objs.length; i++) {
            is_array[i] = ClassUtil.isArray(objs[i]);
        } // FOR
        return (is_array);
    }

    /**
     * Convert a Enum array to a Field array This assumes that the name of each
     * Enum element corresponds to a data member in the clas
     * 
     * @param <E>
     * @param clazz
     * @param members
     * @return
     * @throws NoSuchFieldException
     */
    public static <E extends Enum<?>> Field[] getFieldsFromMembersEnum(Class<?> clazz, E members[]) throws NoSuchFieldException {
        Field fields[] = new Field[members.length];
        for (int i = 0; i < members.length; i++) {
            fields[i] = clazz.getDeclaredField(members[i].name().toLowerCase());
        } // FOR
        return (fields);
    }

    /**
     * Create a mapping from Field handles to their corresponding Annotation
     * @param <A>
     * @param fields
     * @param annotationClass
     * @return
     */
    public static <A extends Annotation> Map<Field, A> getFieldAnnotations(Field fields[], Class<A> annotationClass) {
        Map<Field, A> ret = new LinkedHashMap<Field, A>();
        for (Field f : fields) {
            A a = f.getAnnotation(annotationClass);
            if (a != null)
                ret.put(f, a);
        }
        return (ret);
    }

    /**
     * Get the generic types for the given field
     * 
     * @param field
     * @return
     */
    public static List<Class<?>> getGenericTypes(Field field) {
        ArrayList<Class<?>> generic_classes = new ArrayList<Class<?>>();
        Type gtype = field.getGenericType();
        if (gtype instanceof ParameterizedType) {
            ParameterizedType ptype = (ParameterizedType) gtype;
            getGenericTypesImpl(ptype, generic_classes);
        }
        return (generic_classes);
    }

    private static void getGenericTypesImpl(ParameterizedType ptype, List<Class<?>> classes) {
        // list the actual type arguments
        for (Type t : ptype.getActualTypeArguments()) {
            if (t instanceof Class<?>) {
                // System.err.println("C: " + t);
                classes.add((Class<?>) t);
            } else if (t instanceof ParameterizedType) {
                ParameterizedType next = (ParameterizedType) t;
                // System.err.println("PT: " + next);
                classes.add((Class<?>) next.getRawType());
                getGenericTypesImpl(next, classes);
            }
        } // FOR
        return;
    }

    /**
     * Return an ordered list of all the sub-classes for a given class Useful
     * when dealing with generics
     * 
     * @param element_class
     * @return
     */
    public static List<Class<?>> getSuperClasses(Class<?> element_class) {
        List<Class<?>> ret = ClassUtil.CACHE_getSuperClasses.get(element_class);
        if (ret == null) {
            ret = new ArrayList<Class<?>>();
            while (element_class != null) {
                ret.add(element_class);
                element_class = element_class.getSuperclass();
            } // WHILE
            ret = Collections.unmodifiableList(ret);
            ClassUtil.CACHE_getSuperClasses.put(element_class, ret);
        }
        return (ret);
    }

    /**
     * Get a set of all of the interfaces that the element_class implements
     * 
     * @param element_class
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Collection<Class<?>> getInterfaces(Class<?> element_class) {
        Set<Class<?>> ret = ClassUtil.CACHE_getInterfaceClasses.get(element_class);
        if (ret == null) {
            // ret = new HashSet<Class<?>>();
            // Queue<Class<?>> queue = new LinkedList<Class<?>>();
            // queue.add(element_class);
            // while (!queue.isEmpty()) {
            // Class<?> current = queue.poll();
            // for (Class<?> i : current.getInterfaces()) {
            // ret.add(i);
            // queue.add(i);
            // } // FOR
            // } // WHILE
            ret = new HashSet<Class<?>>(ClassUtils.getAllInterfaces(element_class));
            if (element_class.isInterface())
                ret.add(element_class);
            ret = Collections.unmodifiableSet(ret);
            ClassUtil.CACHE_getInterfaceClasses.put(element_class, ret);
        }
        return (ret);
    }

    @SuppressWarnings("unchecked")
    public static <T> T newInstance(String class_name, Object params[], Class<?> classes[]) {
        return ((T) ClassUtil.newInstance(ClassUtil.getClass(class_name), params, classes));
    }

    public static <T> T newInstance(Class<T> target_class, Object params[], Class<?> classes[]) {
        // Class<?> const_params[] = new Class<?>[params.length];
        // for (int i = 0; i < params.length; i++) {
        // const_params[i] = params[i].getClass();
        // System.err.println("[" + i + "] " + params[i] + " " +
        // params[i].getClass());
        // } // FOR

        Constructor<T> constructor = ClassUtil.getConstructor(target_class, classes);
        T ret = null;
        try {
            ret = constructor.newInstance(params);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create new instance of " + target_class.getSimpleName(), ex);
        }
        return (ret);
    }

    /**
     * Grab the constructor for the given target class with the provided input parameters.
     * This method will first try to find an exact match for the parameters, and if that
     * fails then it will be smart and try to find one with the input parameters super classes. 
     * @param <T>
     * @param target_class
     * @param params
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> Constructor<T> getConstructor(Class<T> target_class, Class<?>... params) {
        NoSuchMethodException error = null;
        try {
            return (target_class.getConstructor(params));
        } catch (NoSuchMethodException ex) {
            // The first time we get this it can be ignored
            // We'll try to be nice and find a match for them
            error = ex;
        }
        assert (error != null);

        if (debug.val) {
            LOG.debug("TARGET CLASS:  " + target_class);
            LOG.debug("TARGET PARAMS: " + Arrays.toString(params));
        }

        final int num_params = (params != null ? params.length : 0);
        List<Class<?>> paramSuper[] = (List<Class<?>>[]) new List[num_params];
        for (int i = 0; i < num_params; i++) {
            paramSuper[i] = ClassUtil.getSuperClasses(params[i]);
            if (debug.val)
                LOG.debug("  SUPER[" + params[i].getSimpleName() + "] => " + paramSuper[i]);
        } // FOR

        for (Constructor<?> c : target_class.getConstructors()) {
            Class<?> cTypes[] = c.getParameterTypes();
            if (debug.val) {
                LOG.debug("CANDIDATE: " + c);
                LOG.debug("CANDIDATE PARAMS: " + Arrays.toString(cTypes));
            }
            if (params.length != cTypes.length)
                continue;

            for (int i = 0; i < num_params; i++) {
                List<Class<?>> cSuper = ClassUtil.getSuperClasses(cTypes[i]);
                if (debug.val)
                    LOG.debug("  SUPER[" + cTypes[i].getSimpleName() + "] => " + cSuper);
                if (CollectionUtils.intersection(paramSuper[i], cSuper).isEmpty() == false) {
                    return ((Constructor<T>) c);
                }
            } // FOR (param)
        } // FOR (constructors)
        throw new RuntimeException("Failed to retrieve constructor for " + target_class.getSimpleName(), error);
    }

    /**
     * @param class_name
     * @return
     */
    public static Class<?> getClass(String class_name) {
        Class<?> target_class = null;
        try {
            ClassLoader loader = ClassLoader.getSystemClassLoader();
            target_class = (Class<?>) loader.loadClass(class_name);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to retrieve class for " + class_name, ex);
        }
        return (target_class);

    }
}

package edu.brown.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.lang.ClassUtils;

/**
 * 
 * @author pavlo
 *
 */
public abstract class ClassUtil {
    
    private static final Map<Class<?>, List<Class<?>>> CACHE_getSuperClasses = new HashMap<Class<?>, List<Class<?>>>(); 
    private static final Map<Class<?>, Set<Class<?>>> CACHE_getInterfaceClasses = new HashMap<Class<?>, Set<Class<?>>>();

    /**
     * Check if the given object is an array (primitve or native).
     * http://www.java2s.com/Code/Java/Reflection/Checkifthegivenobjectisanarrayprimitveornative.htm
     * @param obj  Object to test.
     * @return     True of the object is an array.
     */
    public static boolean isArray(final Object obj) {
        return (obj != null ? obj.getClass().isArray() : false);
    }

    /**
     * Create a mapping from Field handles to their corresponding Annotation
     * @param <A>
     * @param fields
     * @param annotationClass
     * @return
     */
    public static <A extends Annotation> Map<Field, A> getFieldAnnotations(Field fields[], Class<A> annotationClass) {
        Map<Field, A> ret = new ListOrderedMap<Field, A>();
        for (Field f : fields) {
            A a = f.getAnnotation(annotationClass);
            if (a != null) ret.put(f, a);
        }
        return (ret);
    }
    
    /**
     * Get the generic types for the given field
     * @param field
     * @return
     */
    @SuppressWarnings("unchecked")
    public static List<Class> getGenericTypes(Field field) {
        ArrayList<Class> generic_classes = new ArrayList<Class>();
        Type gtype = field.getGenericType();
        if (gtype instanceof ParameterizedType) {
            ParameterizedType ptype = (ParameterizedType)gtype;
            getGenericTypesImpl(ptype, generic_classes);
        }
        return (generic_classes);
    }
        
    @SuppressWarnings("unchecked")
    private static void getGenericTypesImpl(ParameterizedType ptype, List<Class> classes) {
        // list the actual type arguments
        for (Type t : ptype.getActualTypeArguments()) {
            if (t instanceof Class) {
//                System.err.println("C: " + t);
                classes.add((Class)t);
            } else if (t instanceof ParameterizedType) {
                ParameterizedType next = (ParameterizedType)t;
//                System.err.println("PT: " + next);
                classes.add((Class)next.getRawType());
                getGenericTypesImpl(next, classes);
            }
        } // FOR
        return;
    }
    
    /**
     * Return an ordered list of all the sub-classes for a given class
     * Useful when dealing with generics
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
     * @param element_class
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Set<Class<?>> getInterfaces(Class<?> element_class) {
        Set<Class<?>> ret = ClassUtil.CACHE_getInterfaceClasses.get(element_class);
        if (ret == null) {
//            ret = new HashSet<Class<?>>();
//            Queue<Class<?>> queue = new LinkedList<Class<?>>();
//            queue.add(element_class);
//            while (!queue.isEmpty()) {
//                Class<?> current = queue.poll();
//                for (Class<?> i : current.getInterfaces()) {
//                    ret.add(i);
//                    queue.add(i);
//                } // FOR
//            } // WHILE
            ret = new HashSet<Class<?>>(ClassUtils.getAllInterfaces(element_class));
            if (element_class.isInterface()) ret.add(element_class);
            ret = Collections.unmodifiableSet(ret);
            ClassUtil.CACHE_getInterfaceClasses.put(element_class, ret);
        }
        return (ret);
    }
    
    @SuppressWarnings("unchecked")
    public static <T> T newInstance(String class_name, Object params[], Class<?> classes[]) {
        return ((T)ClassUtil.newInstance(ClassUtil.getClass(class_name), params, classes));
    }

    
    public static <T> T newInstance(Class<T> target_class, Object params[], Class<?> classes[]) {
//        Class<?> const_params[] = new Class<?>[params.length];
//        for (int i = 0; i < params.length; i++) {
//            const_params[i] = params[i].getClass();
//            System.err.println("[" + i + "] " + params[i] + " " + params[i].getClass());
//        } // FOR
        
        Constructor<T> constructor = ClassUtil.getConstructor(target_class, classes);
        T ret = null;
        try {
            ret = constructor.newInstance(params);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return (ret);
    }
    
    /**
     * 
     * @param <T>
     * @param target_class
     * @param params
     * @return
     */
    public static <T> Constructor<T> getConstructor(Class<T> target_class, Class<?>...params) {
        Constructor<T> constructor = null;
        try {
//            System.err.println("Looking for constructor: " + target_class);
//            System.err.print("Parameters: ");
//            String add = "";
//            for (Class<?> p_class : params) {
//                System.err.print(add + p_class.getSimpleName());
//                add = ", ";
//            }
//            System.err.println();
            
            constructor = target_class.getConstructor(params); 
        } catch (Exception ex) {
            System.err.println("Failed to retrieve constructor for " + target_class.getSimpleName());
            ex.printStackTrace();
            System.exit(1);
        }
        return (constructor);
    }
    
    /**
     * 
     * @param class_name
     * @return
     */
    public static Class<?> getClass(String class_name) {
        Class<?> target_class = null;
        try {
            ClassLoader loader = ClassLoader.getSystemClassLoader();
            target_class = (Class<?>)loader.loadClass(class_name);
        } catch (Exception ex) {
            System.err.println("Failed to retrieve class for " + class_name);
            ex.printStackTrace();
            System.exit(1);
        }
        return (target_class);
 
    }
}

package edu.brown.hstore.conf;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ConfigProperty {
    /**
     * Description of the configuration property
     * @return
     */
    String description();
    /**
     * Default String value of the configuration property
     * @return
     */
    String defaultString() default "";
    
    /**
     * 
     * @return
     */
    int defaultInt() default Integer.MIN_VALUE;
    
    /**
     * 
     * @return
     */
    long defaultLong() default Long.MIN_VALUE;
    
    /**
     * 
     * @return
     */
    double defaultDouble() default Double.MIN_VALUE;
    
    /**
     * 
     * @return
     */
    boolean defaultBoolean() default false;
    
    /**
     * Whether the default value is null
     * @return
     */
    boolean defaultNull() default false;
    
    /**
     * Whether support for this configuration is considered experimental 
     * @return
     */
    boolean experimental() default false;
    
    /**
     * The new HStoreConf parameter that this one has been replaced with
     * @return
     */
    String replacedBy() default "";
    
    /**
     * The enum class to use to validate options.
     */
    String enumOptions() default "";
}

/**
 * 
 */
package com.mytest.benchmark.abc;

import com.mytest.benchmark.abc.procedures.GetTableCounts;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.BenchmarkComponent;

/**
 * @author mimosally
 *
 */
public class ABCProjectBuilder extends AbstractProjectBuilder {
	
	 public static final Class<? extends BenchmarkComponent> m_clientClass = ABCClient.class;
	 public static final Class<? extends BenchmarkComponent> m_loaderClass = ABCLoader.class;
	 public static final Class<?> PROCEDURES[] = new Class<?>[] {
	        GetTableCounts.class,
	    };
	 public static final String PARTITIONING[][] = new String[][] {
	        // { "TABLE NAME", "PARTITIONING COLUMN NAME" }
	        {"TABLEA", "A_ID"}, 
	        {"TABLEB", "B_A_ID"},
	    };


	 public ABCProjectBuilder() {
	        super("abc", ABCProjectBuilder.class, PROCEDURES, PARTITIONING);
	 
	      
	    }

}

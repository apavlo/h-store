package edu.brown.benchmark.articles;
import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.articles.procedures.*;

public class ArticlesProjectBuilder extends AbstractProjectBuilder{
	 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = ArticlesClient.class;
 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = ArticlesLoader.class;
 
    public static final Class<?> PROCEDURES[] = new Class<?>[] {
        GetArticle.class,
    };
    public static final String PARTITIONING[][] = new String[][] {
        // { "TABLE NAME", "PARTITIONING COLUMN NAME" }
        {"TABLEA", "A_ID"},
        {"TABLEB", "B_A_ID"},
    };
 
    @SuppressWarnings("unchecked")
	public ArticlesProjectBuilder() {
    	super("Articles", ArticlesProjectBuilder.class, (Class<? extends VoltProcedure>[]) PROCEDURES, PARTITIONING);
 
    }

}


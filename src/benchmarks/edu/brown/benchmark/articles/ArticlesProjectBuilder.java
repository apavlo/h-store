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
    {
        // Transaction Frequencies
        addTransactionFrequency(GetArticle.class, ArticlesConstants.FREQUENCY_GET_ARTICLE);
    }

    public static final String PARTITIONING[][] = new String[][] {
        { ArticlesConstants.TABLENAME_ARTICLES, "A_ID" },
        { ArticlesConstants.TABLENAME_USERS, "U_ID" },
        { ArticlesConstants.TABLENAME_COMMENTS, "A_ID" }
    };
 
    @SuppressWarnings("unchecked")
	public ArticlesProjectBuilder() {
    	super("Articles", ArticlesProjectBuilder.class, (Class<? extends VoltProcedure>[]) PROCEDURES, PARTITIONING);
 
    }

}

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
    
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[]) new Class<?>[] {
        GetArticle.class,
        GetComments.class,
        AddComment.class,
        UpdateUserInfo.class
    };
    {
        // Transaction Frequencies
        addTransactionFrequency(GetArticle.class, ArticlesConstants.FREQUENCY_GET_ARTICLE);
        addTransactionFrequency(GetComments.class, ArticlesConstants.FREQUENCY_GET_COMMENTS);
        addTransactionFrequency(AddComment.class, ArticlesConstants.FREQUENCY_ADD_COMMENT);
        addTransactionFrequency(UpdateUserInfo.class, ArticlesConstants.FREQUENCY_UPDATE_USER_INFO);
        
    }

    public static final String PARTITIONING[][] = new String[][] {
        { ArticlesConstants.TABLENAME_ARTICLES, "A_ID" },
        { ArticlesConstants.TABLENAME_USERS, "U_ID" },
        { ArticlesConstants.TABLENAME_COMMENTS, "C_A_ID" }
    };
    
    public ArticlesProjectBuilder() {
        super("articles", ArticlesProjectBuilder.class, PROCEDURES, PARTITIONING);
 
    }

}

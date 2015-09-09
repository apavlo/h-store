package edu.brown.benchmark.articles;

public abstract class ArticlesConstants {

    // ----------------------------------------------------------------
    // STORED PROCEDURE EXECUTION FREQUENCIES (0-100)
    // ----------------------------------------------------------------

    public static final int FREQUENCY_GET_ARTICLE = 35; 
    public static final int FREQUENCY_ADD_COMMENT = 20;
    public static final int FREQUENCY_GET_COMMENTS = 35;
    public static final int FREQUENCY_UPDATE_USER_INFO = 10;

    // ----------------------------------------------------------------
    // DATA CONFIGURATION
    // ----------------------------------------------------------------
    
    public static final int ARTICLES_SIZE = 100000;
    public static final int USERS_SIZE = 200;
    
    public static final int BATCH_SIZE = 500;

    public static final long MAX_COMMENTS_PER_ARTICLE = 1000; // 2^16 - 1
    public static final double COMMENTS_PER_ARTICLE_SIGMA = 2d;
    public static final int COMMENTS_NUM_COLUMNS = 0;
    
    // ----------------------------------------------------------------
    // TABLE NAMES
    // ----------------------------------------------------------------
    public static final String TABLENAME_ARTICLES = "ARTICLES";
    public static final String TABLENAME_USERS = "USERS";
    public static final String TABLENAME_COMMENTS = "COMMENTS";

    public static final String TABLENAMES[] = {
        TABLENAME_ARTICLES,
        TABLENAME_USERS,
        TABLENAME_COMMENTS
    };
}

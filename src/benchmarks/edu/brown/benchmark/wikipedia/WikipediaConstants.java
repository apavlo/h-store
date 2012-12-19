package edu.brown.benchmark.wikipedia;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public abstract class WikipediaConstants {

    /**
     * The percentage of page updates that are made by anonymous users [0%-100%]
     */
    public static final int ANONYMOUS_PAGE_UPDATE_PROB = 26; 
    
    /**
     * 
     */
    public static final int ANONYMOUS_USER_ID = 0;
    
    public static final double USER_ID_SIGMA = 1.0001d;
    
	/**
	 * Length of the tokens
	 */
	public static final int TOKEN_LENGTH = 32;

	/**
	 * Number of baseline pages
	 */
	public static final int PAGES = 10000;

	/**
	 * Number of baseline Users
	 */
	public static final int USERS = 2000;
	
	public static final String UPDATEPAGE_LOG_TYPE = "patrol";
	
	public static final String UPDATEPAGE_LOG_ACTION = "patrol";
	
	public static final int CLIENT_NEXT_ID_OFFSET = 10000;
	
    // -----------------------------------------------------------------
    // GET PAGE OUTPUT COLUMNS
    // -----------------------------------------------------------------
    
    public static final VoltTable.ColumnInfo[] GETPAGE_OUTPUT_COLS = {
        new VoltTable.ColumnInfo("PAGE_ID", VoltType.BIGINT),
        new VoltTable.ColumnInfo("PAGE_TITLE", VoltType.STRING),
        new VoltTable.ColumnInfo("OLD_TEXT", VoltType.STRING),
        new VoltTable.ColumnInfo("TEXT_ID", VoltType.INTEGER),
        new VoltTable.ColumnInfo("REVISION_ID", VoltType.INTEGER),
        new VoltTable.ColumnInfo("USER_TEXT", VoltType.STRING),
    };
    
    // -----------------------------------------------------------------
    // GET UPDATE USER&PAGE COLUMNS
    // -----------------------------------------------------------------
    
    public static final VoltTable.ColumnInfo[] GET_USER_PAGE_UPDATE_COLS = {
        new VoltTable.ColumnInfo("USER_TEXT", VoltType.INTEGER),
        new VoltTable.ColumnInfo("PAGE_ID", VoltType.INTEGER),
    };
    
    // -----------------------------------------------------------------
    // GET UPDATE USER&PAGE COLUMNS
    // -----------------------------------------------------------------
    
    public static final VoltTable.ColumnInfo[] GET_TRACE_COLS = {
        new VoltTable.ColumnInfo("USER_ID", VoltType.INTEGER),
        new VoltTable.ColumnInfo("NAMESPACE", VoltType.INTEGER),
        new VoltTable.ColumnInfo("TITLE", VoltType.STRING),
    };
	
    // ----------------------------------------------------------------
	// DISTRIBUTION CONSTANTS
    // ----------------------------------------------------------------

	public static final double NUM_WATCHES_PER_USER_SIGMA = 1.75d;
	
	public static final double WATCHLIST_PAGE_SIGMA = 1.0001d;
	
	public static final double REVISION_USER_SIGMA = 1.0001d;
	
	public static final int TRACE_DATA_SCALE = 10000;
    
    /**
     * How likely is a user to look at past revision
     */
    public static final double PAST_REV_CHECK_PROB = 0.03;
    
    public static final double PAST_REV_ZIPF_SKEW = 4.0;
	
    // ----------------------------------------------------------------
    // DATA SET INFORMATION
    // ----------------------------------------------------------------
    
    /**
     * Table Names
     */
	public static final String TABLENAME_IPBLOCKS          = "IPBLOCKS";
	public static final String TABLENAME_LOGGING           = "LOGGING";
	public static final String TABLENAME_PAGE              = "PAGE";
	public static final String TABLENAME_PAGE_BACKUP       = "PAGE_BACKUP";
	public static final String TABLENAME_PAGE_RESTRICTIONS = "PAGE_RESTRICTIONS";
	public static final String TABLENAME_RECENTCHANGES     = "RECENTCHANGES";
	public static final String TABLENAME_REVISION          = "REVISION";
	public static final String TABLENAME_TEXT              = "TEXT";
	public static final String TABLENAME_USER              = "USERACCT";
	public static final String TABLENAME_USER_GROUPS       = "USER_GROUPS";
	public static final String TABLENAME_VALUE_BACKUP      = "VALUE_BACKUP";
	public static final String TABLENAME_WATCHLIST         = "WATCHLIST";
	
	// ----------------------------------------------------------------
    // STORED PROCEDURE INFORMATION
    // ----------------------------------------------------------------

    public static final int FREQUENCY_ADD_WATCHLIST = 1;
    public static final int FREQUENCY_REMOVE_WATCHLIST = 1;
    public static final int FREQUENCY_UPDATE_PAGE = 8;
    public static final int FREQUENCY_GET_PAGE_ANONYMOUS = 89;
    public static final int FREQUENCY_GET_PAGE_AUTHENTICATED = 1;
    
	public static final int BATCH_SIZE = 1000;

}

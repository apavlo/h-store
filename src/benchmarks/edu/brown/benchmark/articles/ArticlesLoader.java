package edu.brown.benchmark.articles;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import edu.brown.api.BenchmarkComponent;
import edu.brown.api.Loader;
import edu.brown.catalog.CatalogUtil;

public class ArticlesLoader extends Loader{
    private static final Logger LOG = Logger.getLogger(ArticlesLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    private long articlesSize;
	private long usersSize;
	private long maxComments;
	private HashMap<Long, Long> articleToCommentMap;
    public static void main(String[] args) {
        BenchmarkComponent.main(ArticlesLoader.class, args, true);
    }

    public ArticlesLoader(String[] args) {
        super(args);
        this.articlesSize = Math.round(ArticlesConstants.ARTICLES_SIZE * this.getScaleFactor());
        this.usersSize = Math.round(ArticlesConstants.USERS_SIZE * this.getScaleFactor());
        this.maxComments = 10;
        this.articleToCommentMap = new HashMap<Long, Long>();
    }

    @Override
    public void load() {
        if (d) LOG.debug("Starting ArticlesLoader");
        final Database catalog_db = this.getCatalogContext().database;

        if (d) LOG.debug("Start loading " + ArticlesConstants.TABLENAME_ARTICLES);
        Table catalog_tbl = catalog_db.getTables().get(ArticlesConstants.TABLENAME_ARTICLES);
        genArticles(catalog_tbl);
        if (d) LOG.debug("Finished loading " + ArticlesConstants.TABLENAME_ARTICLES);
                
        if (d) LOG.debug("Start loading " + ArticlesConstants.TABLENAME_USERS);
        catalog_tbl = catalog_db.getTables().get(ArticlesConstants.TABLENAME_USERS);
        genUsers(catalog_tbl);
        if (d) LOG.debug("Finished loading " + ArticlesConstants.TABLENAME_USERS);

        try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        if (d) LOG.debug("Start loading " + ArticlesConstants.TABLENAME_COMMENTS);
        Table catalog_spe = catalog_db.getTables().get(ArticlesConstants.TABLENAME_COMMENTS);
        genComments(catalog_spe);
        if (d) LOG.debug("Finished loading " + ArticlesConstants.TABLENAME_COMMENTS);

    }


    /**
     * Populate Articles table per benchmark spec.
     */
    void genArticles(Table catalog_tbl) {
        final VoltTable table = CatalogUtil.getVoltTable(catalog_tbl);
        Object row[] = new Object[table.getColumnCount()];
        long total = 0;
        for (long a_id = 0; a_id < this.articlesSize; a_id++) {
            int col = 0;
            row[col++] = a_id;
            row[col++] = ArticlesUtil.astring(100, 100); // title
            row[col++] = ArticlesUtil.astring(100, 100); // text
            long numComments = ArticlesUtil.number(0, this.maxComments);
            row[col++] = numComments; // number of comments
            
            this.articleToCommentMap.put(a_id, numComments);
            assert col == table.getColumnCount();
            table.addRow(row);
            total++;

            if (table.getRowCount() >= ArticlesConstants.BATCH_SIZE) {
                if (d) LOG.debug(String.format("%s: %6d / %d",
                                 ArticlesConstants.TABLENAME_ARTICLES, total, this.articlesSize));
                loadVoltTable(ArticlesConstants.TABLENAME_ARTICLES, table);
                table.clearRowData();
                assert(table.getRowCount() == 0);
            }
        } // FOR
        if (table.getRowCount() > 0) {
            if (d) LOG.debug(String.format("%s: %6d / %d",
            		ArticlesConstants.TABLENAME_ARTICLES, total, this.articlesSize));
            loadVoltTable(ArticlesConstants.TABLENAME_ARTICLES, table);
            table.clearRowData();
            assert(table.getRowCount() == 0);
        }
    }

    /**
     * Populate Users table per benchmark spec.
     */
    void genUsers(Table catalog_tbl) {
        final VoltTable table = CatalogUtil.getVoltTable(catalog_tbl);

/*-- u_firstname      User's first name
-- u_lastname       User's last name
-- u_password       User's password
-- u_email          User's email

 * 
 * */                
        long total = 0;
        for (long s_id = 0; s_id < this.usersSize; s_id++) {
                Object row[] = new Object[table.getColumnCount()];
                row[0] = s_id;
                row[1] = ArticlesUtil.astring(3, 3);
                row[2] = ArticlesUtil.astring(3, 3);
                row[3] = ArticlesUtil.astring(3, 3);
                row[4] = ArticlesUtil.astring(3, 3);
                table.addRow(row);
                total++;
            if (table.getRowCount() >= ArticlesConstants.BATCH_SIZE) {
                if (d) LOG.debug(String.format("%s: %6d / %d",
                		ArticlesConstants.TABLENAME_USERS, total, usersSize));
                loadVoltTable(ArticlesConstants.TABLENAME_USERS, table);
                table.clearRowData();
            }
        } // WHILE
        if (table.getRowCount() > 0) {
            if (d) LOG.debug(String.format("%s: %6d / %d",
            		ArticlesConstants.TABLENAME_USERS, total, usersSize));
            loadVoltTable(ArticlesConstants.TABLENAME_USERS, table);
            table.clearRowData();
        }
    }

    /**
     * Populate Comments table per benchmark spec.
     */
    void genComments(Table catalog_comments) {

    	/*
    	 *  -- c_id            Comment's ID
			-- a_id            Article's ID
			-- u_id            User's ID
			-- c_text            Actual comment text
    	 * */
        VoltTable speTbl = CatalogUtil.getVoltTable(catalog_comments);

        long speTotal = 0;
        long c_id = 0;
        for (long a_id = 0; a_id < this.articlesSize; a_id++) {
        	Long numComments = this.articleToCommentMap.get(a_id);
        	for (long i = 0 ; i < numComments; i++){
                Object row_spe[] = new Object[speTbl.getColumnCount()];
                row_spe[0] = c_id++;
                row_spe[1] = a_id; // random number from the article id
                row_spe[2] = ArticlesUtil.number(0, this.usersSize); // random number from user id
                row_spe[3] = ArticlesUtil.astring(5, 5); // comment
                speTbl.addRow(row_spe);
                speTotal++;        		
                if (speTbl.getRowCount() >= ArticlesConstants.BATCH_SIZE) {
                    if (d) LOG.debug(String.format("%s: %d", ArticlesConstants.TABLENAME_COMMENTS, speTotal));
                    loadVoltTable(ArticlesConstants.TABLENAME_COMMENTS, speTbl);
                    speTbl.clearRowData();
                    assert(speTbl.getRowCount() == 0);
                }
        	}
        } // WHILE
        if (speTbl.getRowCount() > 0) {
            if (d) LOG.debug(String.format("%s: %d", ArticlesConstants.TABLENAME_COMMENTS, speTotal));
            loadVoltTable(ArticlesConstants.TABLENAME_COMMENTS, speTbl);
            speTbl.clearRowData();
            assert(speTbl.getRowCount() == 0);
        }
    }
	 	 
	}


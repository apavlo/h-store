package edu.brown.benchmark.articles;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.utils.Pair;

import edu.brown.api.BenchmarkComponent;
import edu.brown.api.Loader;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObservableExceptionHandler;
import edu.brown.utils.EventObserver;

public class ArticlesLoader extends Loader{
    private static final Logger LOG = Logger.getLogger(ArticlesLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    private final boolean blocking = false;
	private long articlesSize;
	private long usersSize;
    public static void main(String[] args) {
        BenchmarkComponent.main(ArticlesLoader.class, args, true);
    }

    public ArticlesLoader(String[] args) {
        super(args);
        this.articlesSize = Math.round(ArticlesConstants.ARTICLES_SIZE * this.getScaleFactor());
        this.usersSize = Math.round(ArticlesConstants.USERS_SIZE * this.getScaleFactor());
    }

    @Override
    public void load() {
        if (d) LOG.debug("Starting ArticlesLoader");
        final Database catalog_db = this.getCatalogContext().database;

        final Thread threads[] =
        { new Thread() {
            public void run() {
                if (d) LOG.debug("Start loading " + ArticlesConstants.TABLENAME_ARTICLES);
                Table catalog_tbl = catalog_db.getTables().get(ArticlesConstants.TABLENAME_ARTICLES);
                genArticles(catalog_tbl);
                if (d) LOG.debug("Finished loading " + ArticlesConstants.TABLENAME_ARTICLES);
            }
        }, new Thread() {
            public void run() {
                if (d) LOG.debug("Start loading " + ArticlesConstants.TABLENAME_USERS);
                Table catalog_tbl = catalog_db.getTables().get(ArticlesConstants.TABLENAME_USERS);
                genUsers(catalog_tbl);
                if (d) LOG.debug("Finished loading " + ArticlesConstants.TABLENAME_USERS);
            }
        }, new Thread() {
            public void run() {
                if (d) LOG.debug("Start loading " + ArticlesConstants.TABLENAME_COMMENTS);
                Table catalog_spe = catalog_db.getTables().get(ArticlesConstants.TABLENAME_COMMENTS);
                genComments(catalog_spe);
                if (d) LOG.debug("Finished loading " + ArticlesConstants.TABLENAME_COMMENTS);
            }
        } };

        final EventObservableExceptionHandler handler = new EventObservableExceptionHandler();
        handler.addObserver(new EventObserver<Pair<Thread,Throwable>>() {
            @Override
            public void update(EventObservable<Pair<Thread, Throwable>> o, Pair<Thread, Throwable> t) {
                for (Thread thread : threads)
                    thread.interrupt();
            }
        });
        
        try {
            for (Thread t : threads) {
                t.setUncaughtExceptionHandler(handler);
                t.start();
                if (this.blocking)
                    t.join();
            } // FOR
            if (!this.blocking) {
                for (Thread t : threads)
                    t.join();
            }
            this.getClientHandle().drain();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (handler.hasError()) {
                throw new RuntimeException("Error while generating table data.", handler.getError());
            }
        }

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
            row[col++] = 0; // number of comments
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
        for (long c_id = 0; c_id < this.articlesSize; c_id++) {
            Object row_spe[] = new Object[speTbl.getColumnCount()];
            row_spe[0] = c_id;
            row_spe[1] = ArticlesUtil.number(0, this.articlesSize); // random number from the article id
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
        } // WHILE
        if (speTbl.getRowCount() > 0) {
            if (d) LOG.debug(String.format("%s: %d", ArticlesConstants.TABLENAME_COMMENTS, speTotal));
            loadVoltTable(ArticlesConstants.TABLENAME_COMMENTS, speTbl);
            speTbl.clearRowData();
            assert(speTbl.getRowCount() == 0);
        }
    }
	 	 
	}


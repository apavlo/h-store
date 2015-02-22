package edu.brown.benchmark.articles;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import edu.brown.api.Loader;
import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.RandomDistribution.Gaussian;
import edu.brown.rand.RandomDistribution.Zipf;

public class ArticlesLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(ArticlesLoader.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private long articlesSize;
    private long usersSize;
    
    private final Random rand = new Random();
    private final Zipf numComments;
    private final HashMap<Long, Integer> articleToCommentMap;

    public ArticlesLoader(String[] args) {
        super(args);
        this.numComments = new Zipf(this.rand, 0,
                                    ArticlesConstants.MAX_COMMENTS_PER_ARTICLE,
                                    ArticlesConstants.COMMENTS_PER_ARTICLE_SIGMA); 
        this.articlesSize = Math.round(ArticlesConstants.ARTICLES_SIZE * this.getScaleFactor());
        this.usersSize = Math.round(ArticlesConstants.USERS_SIZE * this.getScaleFactor());
        this.articleToCommentMap = new HashMap<Long, Integer>();
    }

    @Override
    public void load() {
        if (debug.val) LOG.debug("Starting ArticlesLoader");
        final Database catalog_db = this.getCatalogContext().database;

        if (debug.val) LOG.debug("Start loading " + ArticlesConstants.TABLENAME_ARTICLES);
        Table catalog_tbl = catalog_db.getTables().get(ArticlesConstants.TABLENAME_ARTICLES);
        this.genArticles(catalog_tbl);
        if (debug.val) LOG.debug("Finished loading " + ArticlesConstants.TABLENAME_ARTICLES);
                
        if (debug.val) LOG.debug("Start loading " + ArticlesConstants.TABLENAME_USERS);
        catalog_tbl = catalog_db.getTables().get(ArticlesConstants.TABLENAME_USERS);
        this.genUsers(catalog_tbl);
        if (debug.val) LOG.debug("Finished loading " + ArticlesConstants.TABLENAME_USERS);

        if (debug.val) LOG.debug("Start loading " + ArticlesConstants.TABLENAME_COMMENTS);
        Table catalog_spe = catalog_db.getTables().get(ArticlesConstants.TABLENAME_COMMENTS);
        this.genComments(catalog_spe);
        if (debug.val) LOG.debug("Finished loading " + ArticlesConstants.TABLENAME_COMMENTS);

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
            
            // # of Comments
            int numComments = this.numComments.nextInt();
            row[col++] = numComments;
            if (numComments > 0) {
                this.articleToCommentMap.put(a_id, numComments);
            }
            
            row[col++] = ArticlesUtil.astring(8, 8); // text
            row[col++] = ArticlesUtil.astring(8, 8); // text
            row[col++] = ArticlesUtil.astring(8, 8); // text
            row[col++] = ArticlesUtil.astring(8, 8); // text
            row[col++] = ArticlesUtil.astring(8, 8); // text
            row[col++] = ArticlesUtil.astring(8, 8); // text
            row[col++] = ArticlesUtil.astring(8, 8); // text
            row[col++] = ArticlesUtil.astring(8, 8); // text
            row[col++] = ArticlesUtil.astring(8, 8); // text
            row[col++] = ArticlesUtil.astring(8, 8); // text
            row[col++] = ArticlesUtil.astring(8, 8); // text
            row[col++] = ArticlesUtil.astring(8, 8); // text
            row[col++] = ArticlesUtil.astring(8, 8); // text
            row[col++] = ArticlesUtil.astring(8, 8); // text
            row[col++] = ArticlesUtil.astring(8, 8); // text
            
            
//            assert col == table.getColumnCount();
            table.addRow(row);
            total++;

            if (table.getRowCount() >= ArticlesConstants.BATCH_SIZE) {
                if (debug.val) LOG.debug(String.format("%s: %6d / %d",
                                 ArticlesConstants.TABLENAME_ARTICLES, total, this.articlesSize));
                loadVoltTable(ArticlesConstants.TABLENAME_ARTICLES, table);
                table.clearRowData();
                assert(table.getRowCount() == 0);
            }
        } // FOR
        if (table.getRowCount() > 0) {
            if (debug.val) LOG.debug(String.format("%s: %6d / %d",
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

        // Random length generators for columns:
        //  u_firstname
        //  u_lastname
        //  u_password
        //  u_email
        Gaussian stringSizes[] = new Gaussian[4];
        Random rand = new Random();
        for (int i = 0; i < stringSizes.length; i++) {
            Column col = catalog_tbl.getColumns().get(i+1);
            assert(col != null);
            assert(col.getType() == VoltType.STRING.getValue()) : "Unexpected type for " + col;
            stringSizes[i] = new Gaussian(rand, 5, col.getSize());
        }
        
        long total = 0;
        for (long s_id = 0; s_id < this.usersSize; s_id++) {
            int i = 0;
            Object row[] = new Object[table.getColumnCount()];
            row[i++] = s_id;
            for (int j = 0; j < stringSizes.length; j++) {
                int length = stringSizes[j].nextInt();
                row[i++] = ArticlesUtil.astring(length, length);
            } // FOR
            table.addRow(row);
            total++;
            
            if (table.getRowCount() >= ArticlesConstants.BATCH_SIZE) {
                if (debug.val) LOG.debug(String.format("%s: %6d / %d",
                                         ArticlesConstants.TABLENAME_USERS, total, usersSize));
                loadVoltTable(ArticlesConstants.TABLENAME_USERS, table);
                table.clearRowData();
            }
        } // WHILE
        if (table.getRowCount() > 0) {
            if (debug.val) LOG.debug(String.format("%s: %6d / %d",
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
        for (Entry<Long, Integer> e : this.articleToCommentMap.entrySet()) {
            long a_id = e.getKey();
            int numComments = e.getValue();
            
            for (long i = 0 ; i < numComments; i++){
                Object row[] = new Object[speTbl.getColumnCount()];
                row[0] = ArticlesUtil.getCommentId(a_id, i);
                row[1] = a_id;
                row[2] = ArticlesUtil.number(0, this.usersSize); // random number from user id
                row[3] = ArticlesUtil.astring(100, 100); // comment
                
                row[4] = ArticlesUtil.astring(8, 8); // comment
                row[5] = ArticlesUtil.astring(8, 8); // comment
                row[6] = ArticlesUtil.astring(8, 8); // comment
                row[7] = ArticlesUtil.astring(8, 8); // comment
                row[8] = ArticlesUtil.astring(8, 8); // comment
                row[9] = ArticlesUtil.astring(8, 8); // comment
                row[10] = ArticlesUtil.astring(8, 8); // comment
                row[11] = ArticlesUtil.astring(8, 8); // comment
                row[12] = ArticlesUtil.astring(8, 8); // comment
                row[13] = ArticlesUtil.astring(8, 8); // comment
                
                speTbl.addRow(row);
                speTotal++;                
                if (speTbl.getRowCount() >= ArticlesConstants.BATCH_SIZE) {
                    if (debug.val) LOG.debug(String.format("%s: %d", ArticlesConstants.TABLENAME_COMMENTS, speTotal));
                    loadVoltTable(ArticlesConstants.TABLENAME_COMMENTS, speTbl);
                    speTbl.clearRowData();
                    assert(speTbl.getRowCount() == 0);
                }
            }
        } // WHILE
        if (speTbl.getRowCount() > 0) {
            if (debug.val) LOG.debug(String.format("%s: %d", ArticlesConstants.TABLENAME_COMMENTS, speTotal));
            loadVoltTable(ArticlesConstants.TABLENAME_COMMENTS, speTbl);
            speTbl.clearRowData();
            assert(speTbl.getRowCount() == 0);
        }
    }
          
    }


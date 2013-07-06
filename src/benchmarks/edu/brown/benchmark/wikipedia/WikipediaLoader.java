package edu.brown.benchmark.wikipedia;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

import edu.brown.api.Loader;
import edu.brown.benchmark.wikipedia.data.PageHistograms;
import edu.brown.benchmark.wikipedia.data.TextHistograms;
import edu.brown.benchmark.wikipedia.data.UserHistograms;
import edu.brown.benchmark.wikipedia.procedures.UpdateRevisionCounters;
import edu.brown.benchmark.wikipedia.util.TextGenerator;
import edu.brown.benchmark.wikipedia.util.WikipediaUtil;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.rand.RandomDistribution.Zipf;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;

/**
 * Synthetic Wikipedia Data Loader
 * @author pavlo
 * @author djellel
 * @author xin
 */
public class WikipediaLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(WikipediaLoader.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final Random randGenerator = new Random();
    private final WikipediaUtil util;
    
    /**
     * UserId -> # of Revisions
     */
    private final int user_revision_ctr[];

    /**
     * PageId -> Last Revision Id
     */
    private final int page_last_rev_id[];
    
    /**
     * PageId -> Last Revision Length
     */
    private final int page_last_rev_length[];
    
    private final AtomicInteger page_counter = new AtomicInteger(0);
    
    /**
     * Constructor
     * @param benchmark
     * @param c
     */
    public WikipediaLoader(String[] args) {
        super(args);
        this.util = new WikipediaUtil(this.randGenerator, this.getScaleFactor());
        
        this.user_revision_ctr = new int[this.util.num_users];
        this.page_last_rev_id = new int[this.util.num_pages];
        this.page_last_rev_length = new int[this.util.num_pages];
        
        Arrays.fill(this.page_last_rev_id, 0);
        Arrays.fill(this.user_revision_ctr, 0);
        Arrays.fill(this.page_last_rev_length, 0);
        
        if (debug.val) {
            LOG.debug("# of USERS:  " + util.num_users);
            LOG.debug("# of PAGES: " + util.num_pages);
        }
    }
    
    @Override
    public void load() throws IOException {
        final CatalogContext catalogContext = this.getCatalogContext();
        try {
            // Load Data
            this.loadUsers(catalogContext.database);
            this.loadPages(catalogContext.database);
            this.loadWatchlist(catalogContext.database);
            
            // Multiple Threads
            List<Runnable> runnables = new ArrayList<Runnable>();
            int num_threads = ThreadUtil.availableProcessors();
            int pageId = 1;
            int pagesPerThread = (int)Math.ceil(util.num_pages / (double)num_threads);
            for (int i = 0; i < num_threads; i++) {
                final int firstPageId = pageId;
                final int lastPageId = Math.min(util.num_pages, firstPageId + pagesPerThread);
                Runnable r = new Runnable() {
                    @Override
                    public void run() {
                        WikipediaLoader.this.loadRevision(catalogContext.database, firstPageId, lastPageId);
                    }
                };
                runnables.add(r);
                pageId += pagesPerThread;
            } // FOR
            ThreadUtil.runGlobalPool(runnables);
            
            // Update Counters
            this.updateCounters();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    
    private void updateCounters() throws Exception {
        // UPDATE USER & UPDATE PAGES
        Client client = this.getClientHandle();
        ClientResponse cr = client.callProcedure(UpdateRevisionCounters.class.getSimpleName(),
                                                 this.user_revision_ctr,
                                                 util.num_pages,
                                                 this.page_last_rev_id,
                                                 this.page_last_rev_length);
        assert(cr != null);
        assert(cr.getStatus() == Status.OK);
        if (debug.val) LOG.debug("Updated page/user revision counters");
    }
    
    /**
     * USERACCTS
     */
    private void loadUsers(Database catalog_db) {
        Table userTable = catalog_db.getTables().getIgnoreCase(WikipediaConstants.TABLENAME_USER);
        assert(userTable != null);
        

        VoltTable vt = CatalogUtil.getVoltTable(userTable);
        int num_cols = userTable.getColumns().size();
        int batchSize = 0;
        int lastPercent = -1;
        for (int userId = 1; userId <= util.num_users; userId++) {
            // The name will be prefixed with their UserId. This increases
            // the likelihood that all of our usernames are going to be unique
            // It's not a guarantee, but it's good enough...
            String name = Integer.toString(userId) + TextGenerator.randomStr(randGenerator, util.h_nameLength.nextValue().intValue());
            String realName = TextGenerator.randomStr(randGenerator, util.h_realNameLength.nextValue().intValue());
            int revCount = util.h_revCount.nextValue().intValue();
            String password = StringUtil.repeat("*", randGenerator.nextInt(32));
            
            char eChars[] = TextGenerator.randomChars(randGenerator, randGenerator.nextInt(32) + 5);
            eChars[4 + randGenerator.nextInt(eChars.length-4)] = '@';
            String email = new String(eChars);
            
            String token = TextGenerator.randomStr(randGenerator, WikipediaConstants.TOKEN_LENGTH);
            String userOptions = "fake_longoptionslist";
            TimestampType newPassTime = new TimestampType();
            TimestampType touched = new TimestampType();

            Object row[] = new Object[num_cols];
            int param = 0;
            row[param++] = userId;      // user_id
            row[param++] = name;        // user_name
            row[param++] = realName;    // user_real_name
            row[param++] = password;    // user_password
            row[param++] = password;    // user_newpassword
            row[param++] = newPassTime; // user_newpass_time
            row[param++] = email;       // user_email
            row[param++] = userOptions; // user_options
            row[param++] = touched;     // user_touched
            row[param++] = token;       // user_token
            row[param++] = null;        // user_email_authenticated
            row[param++] = null;        // user_email_token
            row[param++] = null;        // user_email_token_expires
            row[param++] = null;        // user_registration
            row[param++] = revCount;    // user_editcount
            vt.addRow(row);

            if (++batchSize % WikipediaConstants.BATCH_SIZE == 0) {
                this.loadVoltTable(userTable.getName(), vt);
                vt.clearRowData();
                batchSize = 0;
                if (debug.val) {
                    int percent = (int) (((double) userId / (double) util.num_users) * 100);
                    if (percent != lastPercent) LOG.debug("USERACCT (" + percent + "%)");
                    lastPercent = percent;
                }
            }
        } // FOR
        if (batchSize > 0) {
            this.loadVoltTable(userTable.getName(), vt);
            vt.clearRowData();
        }
        
        if (debug.val) LOG.debug(userTable.getName() + " Loaded");
    }

    /**
     * PAGE
     */
    private void loadPages(Database catalog_db) {
        Table pageTable = catalog_db.getTables().get(WikipediaConstants.TABLENAME_PAGE);
        assert(pageTable != null);

        VoltTable vt = CatalogUtil.getVoltTable(pageTable);
        int num_cols = pageTable.getColumns().size();
        int batchSize = 0;
        int lastPercent = -1;
        for (long pageId = 1; pageId <= util.num_pages; pageId++) {
            String title = TextGenerator.randomStr(this.randGenerator, util.h_titleLength.nextValue().intValue());
            int namespace = util.getPageNameSpace(pageId);
            String restrictions = util.h_restrictions.nextValue();
            double pageRandom = randGenerator.nextDouble();
            TimestampType pageTouched = new TimestampType();
            
            Object row[] = new Object[num_cols];
            int param = 0;
            row[param++] = pageId;          // page_id
            row[param++] = namespace;       // page_namespace
            row[param++] = title;           // page_title
            row[param++] = restrictions;    // page_restrictions
            row[param++] = 0;               // page_counter
            row[param++] = 0;               // page_is_redirect
            row[param++] = 0;               // page_is_new
            row[param++] = pageRandom;      // page_random
            row[param++] = pageTouched;     // page_touched
            row[param++] = 0;               // page_latest
            row[param++] = 0;               // page_len
            
            vt.addRow(row);

            if (++batchSize % WikipediaConstants.BATCH_SIZE == 0) {
                this.loadVoltTable(pageTable.getName(), vt);
                vt.clearRowData();
                batchSize = 0;
                if (debug.val) {
                    int percent = (int) (((double) pageId / (double) util.num_pages) * 100);
                    if (percent != lastPercent) LOG.debug("PAGE (" + percent + "%)");
                    lastPercent = percent;
                }
            }
        } // FOR
        if (batchSize > 0) {
            this.loadVoltTable(pageTable.getName(), vt);
            vt.clearRowData();
        }
        if (debug.val) LOG.debug(pageTable.getName() + " Loaded");
    }

    /**
     * WATCHLIST
     */
    private void loadWatchlist(Database catalog_db) {
        Table watchTable = catalog_db.getTables().get(WikipediaConstants.TABLENAME_WATCHLIST);
        assert(watchTable != null);
        
        VoltTable vt = CatalogUtil.getVoltTable(watchTable);
        int num_cols = watchTable.getColumns().size();
        int batchSize = 0;
        int lastPercent = -1;
        Set<Long> userPages = new HashSet<Long>();
        for (int user_id = 1; user_id <= util.num_users; user_id++) {
            int num_watches = util.h_watchPageCount.nextInt();
            if (trace.val) LOG.trace(user_id + " => " + num_watches);
            
            userPages.clear();
            for (int i = 0; i < num_watches; i++) {
                long pageId = util.h_watchPageId.nextLong();
                while (userPages.contains(pageId)) {
                    pageId = util.h_watchPageId.nextLong();
                } // WHILE
                userPages.add(pageId);
                int nameSpace = util.getPageNameSpace(pageId);
                
                Object row[] = new Object[num_cols];
                int param = 0;
                row[param++] = user_id;     // wl_user
                row[param++] = nameSpace;   // wl_namespace
                row[param++] = pageId;      // wl_page
                row[param++] = null;        // wl_notificationtimestamp
                vt.addRow(row);
                batchSize++;
            } // FOR

            if (batchSize >= WikipediaConstants.BATCH_SIZE) {
                if (trace.val) LOG.trace("watchList(batch):\n" + vt);
                this.loadVoltTable(watchTable.getName(), vt);
                vt.clearRowData();
                batchSize = 0;
                if (debug.val) {
                    int percent = (int) (((double) user_id / (double) util.num_users) * 100);
                    if (percent != lastPercent) LOG.debug("WATCHLIST (" + percent + "%)");
                    lastPercent = percent;
                }
            }
        } // FOR
        if (batchSize > 0) {
            if (trace.val) LOG.trace("watchList(<batch):\n" + vt);
            this.loadVoltTable(watchTable.getName(), vt);
            vt.clearRowData();
        }
        if (debug.val) LOG.debug(watchTable.getName() + " Loaded");
    }

    /**
     * REVISIONS
     */
    private void loadRevision(Database catalog_db, int firstPageId, long lastPageId) {
        
        // TEXT
        Table textTable = catalog_db.getTables().get(WikipediaConstants.TABLENAME_TEXT);
        assert(textTable != null) : "Failed to find " + WikipediaConstants.TABLENAME_TEXT;
        Column textTableColumn = textTable.getColumns().getIgnoreCase("OLD_TEXT");
        assert(textTableColumn != null) : "Failed to find " + WikipediaConstants.TABLENAME_TEXT + ".OLD_TEXT";
        int max_text_length = textTableColumn.getSize();
        
        // REVISION
        Table revTable = catalog_db.getTables().get(WikipediaConstants.TABLENAME_REVISION);
        assert(revTable != null) : "Failed to find " + WikipediaConstants.TABLENAME_REVISION;
        
        VoltTable vtText = CatalogUtil.getVoltTable(textTable);
        VoltTable vtRev = CatalogUtil.getVoltTable(revTable);
        int num_txt_cols = textTable.getColumns().size();
        int num_rev_cols = revTable.getColumns().size();
        int batchSize = 1;
        
        Zipf h_users = new Zipf(this.randGenerator, 1, util.num_users, WikipediaConstants.REVISION_USER_SIGMA);
        FlatHistogram<Integer> h_textLength = new FlatHistogram<Integer>(this.randGenerator, TextHistograms.TEXT_LENGTH);
        FlatHistogram<Integer> h_nameLength = new FlatHistogram<Integer>(this.randGenerator, UserHistograms.NAME_LENGTH);
        FlatHistogram<Integer> h_numRevisions = new FlatHistogram<Integer>(this.randGenerator, PageHistograms.REVISIONS_PER_PAGE);
        
        int lastPercent = -1;
        for (int pageId = firstPageId; pageId <= lastPageId; pageId++) {
            // There must be at least one revision per page
            int num_revised = h_numRevisions.nextValue().intValue();
            
            // Generate what the new revision is going to be
            int old_text_length = h_textLength.nextValue().intValue();
            if (trace.val) LOG.trace("Max length:" + max_text_length + " old_text_length:" + old_text_length);
            assert(old_text_length > 0);
            assert(old_text_length < max_text_length);
            char old_text[] = TextGenerator.randomChars(randGenerator, old_text_length);
            long batchBytes = 0;
            
            for (int i = 0; i < num_revised; i++) {
                // Generate the User who's doing the revision and the Page revised
                // Makes sure that we always update their counter
                int user_id = h_users.nextInt();
                assert(user_id > 0 && user_id <= util.num_users) : "Invalid UserId '" + user_id + "'";
                this.user_revision_ctr[user_id-1]++;
                TimestampType timestamp = new TimestampType();
                
                // Generate what the new revision is going to be
                if (i > 0) {
                    old_text = util.generateRevisionText(old_text);
                    old_text_length = old_text.length;
                }
                
                int rev_id = ++this.page_last_rev_id[pageId-1];
                this.page_last_rev_length[pageId-1] = old_text_length;
                
                // TEXT
                Object row[] = new Object[num_txt_cols];
                int col = 0;
                row[col++] = rev_id;                // old_id
                row[col++] = new String(old_text);  // old_text
                row[col++] = "utf-8";               // old_flags
                row[col++] = pageId;                // old_page
                vtText.addRow(row);

                // The REV_USER_TEXT field is usually the username, but we'll just 
                // put in gibberish for now
                String user_text = new String(TextGenerator.randomChars(randGenerator, h_nameLength.nextValue().intValue()));
                String rev_comment = new String(TextGenerator.randomChars(randGenerator, util.h_commentLength.nextValue().intValue()));
                int minor_edit = util.h_minorEdit.nextValue().intValue();
                
                // REVISION
                col = 0;
                row = new Object[num_rev_cols];
                row[col++] = rev_id;                // rev_id
                row[col++] = pageId;                // rev_page
                row[col++] = rev_id;                // rev_text_id
                row[col++] = rev_comment;           // rev_comment
                row[col++] = user_id;               // rev_user
                row[col++] = user_text;             // rev_user_text
                row[col++] = timestamp;             // rev_timestamp
                row[col++] = minor_edit;            // rev_minor_edit
                row[col++] = 0;                     // rev_deleted
                row[col++] = old_text.length;       // rev_len
                row[col++] = 0;                     // rev_parent_id
                vtRev.addRow(row);
                
                if (trace.val) LOG.trace(String.format("%s [pageId=%05d / revId=%05d]",
                                                         revTable.getName(), pageId, rev_id));
                batchBytes += old_text.length;
                batchSize++;
                
                if (batchSize > WikipediaConstants.BATCH_SIZE || batchBytes >= 16777216) {
                    this.loadVoltTable(textTable.getName(), vtText);
                    this.loadVoltTable(revTable.getName(), vtRev);
                    vtText.clearRowData();
                    vtRev.clearRowData();
                    batchSize = 0;
                    batchBytes = 0;
                }
            } // FOR (revision)
            
            // XXX: We have to push out the batch for each page, because sometimes we
            // generate a batch that is too large and we lose our connection to the database
            if (batchSize > WikipediaConstants.BATCH_SIZE || batchBytes >= 16777216) {
                this.loadVoltTable(textTable.getName(), vtText);
                this.loadVoltTable(revTable.getName(), vtRev);
                vtText.clearRowData();
                vtRev.clearRowData();
                batchSize = 0;
                batchBytes = 0;
            }
            
            if (debug.val) {
                int percent = (int) (((double) this.page_counter.incrementAndGet() / (double) util.num_pages) * 100);
                if (percent != lastPercent) LOG.debug("REVISIONS (" + percent + "%)");
                lastPercent = percent;
            }
        } // FOR (page)
        if (batchSize > 0) {
            this.loadVoltTable(textTable.getName(), vtText);
            this.loadVoltTable(revTable.getName(), vtRev);
            vtText.clearRowData();
            vtRev.clearRowData();
        }
        
        
        if (debug.val) LOG.debug(textTable.getName() + " Loaded");
        if (debug.val) LOG.debug(revTable.getName() + " Loaded");
    }
   
}
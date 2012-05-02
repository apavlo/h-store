package edu.brown.benchmark.wikipedia;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;

import edu.brown.benchmark.BenchmarkComponent;
import edu.brown.benchmark.wikipedia.data.PageHistograms;
import edu.brown.benchmark.wikipedia.data.RevisionHistograms;
import edu.brown.benchmark.wikipedia.data.TextHistograms;
import edu.brown.benchmark.wikipedia.data.UserHistograms;
import edu.brown.benchmark.wikipedia.procedures.UpdateRevisionCounters;
import edu.brown.benchmark.wikipedia.util.TextGenerator;
import edu.brown.benchmark.wikipedia.util.WikipediaUtil;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.rand.RandomDistribution.Zipf;
import edu.brown.utils.StringUtil;

/**
 * Synthetic Wikipedia Data Loader
 * @author pavlo
 * @author djellel
 * @author xin
 */
public class WikipediaLoader extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(WikipediaLoader.class);

    private final int num_users;
    private final int num_pages;
    
    /**
     *  scale all table cardinalities by this factor
     */
    private final double m_scalefactor;
    
    private Random randGenerator = new Random();
    
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
    
    /**
     * Pair<PageNamespace, PageTitle>
     */
    private List<Pair<Integer, String>> titles = new ArrayList<Pair<Integer, String>>();
    
    /**
     * Constructor
     * @param benchmark
     * @param c
     */
    public WikipediaLoader(String[] args) {
        super(args);
        m_scalefactor = 1.0; // FIXME, scalefactor can be...
        
        this.num_users = (int) Math.round(WikipediaConstants.USERS * this.m_scalefactor);
        this.num_pages = (int) Math.round(WikipediaConstants.PAGES * this.m_scalefactor);
        
        this.user_revision_ctr = new int[this.num_users];
        Arrays.fill(this.user_revision_ctr, 0);
        
        this.page_last_rev_id = new int[this.num_pages];
        Arrays.fill(this.page_last_rev_id, -1);
        this.page_last_rev_length = new int[this.num_pages];
        Arrays.fill(this.page_last_rev_length, -1);
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("# of USERS:  " + this.num_users);
            LOG.debug("# of PAGES: " + this.num_pages);
        }
    }
    
    @Override
    public String[] getTransactionDisplayNames() {
        // IGNORE: Only needed for Client
        return new String[] {};
    }

    @Override
    public void runLoop() {
        final Catalog catalog = this.getCatalog();
        final Database catalog_db = CatalogUtil.getDatabase(catalog);
        
        try {
            // Load Data
            this.loadUsers(catalog_db);
            this.loadPages(catalog_db);
            this.loadWatchlist(catalog_db);
            this.loadRevision(catalog_db);
            LOG.info("Finish loadRevision... ");
            
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * USERACCTS
     */
    private void loadUsers( Database catalog_db ) {
        assert(catalog_db != null);
        Table catalog_tbl = null;
        try {
            catalog_tbl = catalog_db.getTables().get(WikipediaConstants.TABLENAME_USER);
        } catch (Exception e) {
            
            e.printStackTrace();
        }
        LOG.info("TABLES: " + CatalogUtil.debug(catalog_db.getTables()));
        assert(catalog_tbl != null);
        
//        String sql = SQLUtil.getInsertSQL(catalog_tbl);
//        PreparedStatement userInsert = this.conn.prepareStatement(sql);
        FlatHistogram<Integer> h_nameLength = new FlatHistogram<Integer>(this.randGenerator, UserHistograms.NAME_LENGTH);
        FlatHistogram<Integer> h_realNameLength = new FlatHistogram<Integer>(this.randGenerator, UserHistograms.REAL_NAME_LENGTH);
        FlatHistogram<Integer> h_revCount = new FlatHistogram<Integer>(this.randGenerator, UserHistograms.REVISION_COUNT);

        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        int num_cols = catalog_tbl.getColumns().size();
        int batchSize = 0;
        int lastPercent = -1;
        for (int i = 1; i <= this.num_users; i++) {
            // The name will be prefixed with their UserId. This increases
            // the likelihood that all of our usernames are going to be unique
            // It's not a guarantee, but it's good enough...
            String name = Integer.toString(i) + TextGenerator.randomStr(randGenerator, h_nameLength.nextValue().intValue());
            String realName = TextGenerator.randomStr(randGenerator, h_realNameLength.nextValue().intValue());
            int revCount = h_revCount.nextValue().intValue();
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
            row[param++] = i;                // user_id
            row[param++] = name;          // user_name
            row[param++] = realName;      // user_real_name
            row[param++] = password;      // user_password
            row[param++] = password;      // user_newpassword
            row[param++] = newPassTime;   // user_newpass_time
            row[param++] = email;         // user_email
            row[param++] = userOptions;   // user_options
            row[param++] = touched;       // user_touched
            row[param++] = token;         // user_token
            row[param++] = null;    // user_email_authenticated
            row[param++] = null;    // user_email_token
            row[param++] = null;    // user_email_token_expires
            row[param++] = null;    // user_registration
            row[param++] = revCount;         // user_editcount
            vt.addRow(row);

            if (++batchSize % WikipediaConstants.BATCH_SIZE == 0) {
                this.loadVoltTable(catalog_tbl.getName(), vt);
                vt.clearRowData();
                batchSize = 0;
                if (LOG.isDebugEnabled()) {
                    int percent = (int) (((double) i / (double) this.num_users) * 100);
                    if (percent != lastPercent) LOG.debug("USERACCT (" + percent + "%)");
                    lastPercent = percent;
                }
            }
        } // FOR
        if (batchSize > 0) {
            this.loadVoltTable(catalog_tbl.getName(), vt);
            vt.clearRowData();
        }
        
        if (LOG.isDebugEnabled())
            LOG.debug("Users  % " + this.num_users);
    }

    /**
     * PAGE
     */
    private void loadPages(Database catalog_db) {
        Table catalog_tbl = catalog_db.getTables().get(WikipediaConstants.TABLENAME_PAGE);
        assert(catalog_tbl != null);

        FlatHistogram<Integer> h_titleLength = new FlatHistogram<Integer>(this.randGenerator, PageHistograms.TITLE_LENGTH);
        FlatHistogram<Integer> h_namespace = new FlatHistogram<Integer>(this.randGenerator, PageHistograms.NAMESPACE);
        FlatHistogram<String> h_restrictions = new FlatHistogram<String>(this.randGenerator, PageHistograms.RESTRICTIONS);
        
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        int num_cols = catalog_tbl.getColumns().size();
        int batchSize = 0;
        int lastPercent = -1;
        for (int i = 1; i <= this.num_pages; i++) {
            String title = TextGenerator.randomStr(randGenerator, h_titleLength.nextValue().intValue());
            int namespace = h_namespace.nextValue().intValue();
            String restrictions = h_restrictions.nextValue();
            double pageRandom = randGenerator.nextDouble();
            TimestampType pageTouched = new TimestampType();
            
            Object row[] = new Object[num_cols];
            int param = 0;
            row[param++] = i;              // page_id
            row[param++] = namespace;      // page_namespace
            row[param++] = title;       // page_title
            row[param++] = restrictions;// page_restrictions
            row[param++] = 0;              // page_counter
            row[param++] = 0;              // page_is_redirect
            row[param++] = 0;              // page_is_new
            row[param++] = pageRandom;  // page_random
            row[param++] = pageTouched; // page_touched
            row[param++] = 0;              // page_latest
            row[param++] = 0;              // page_len
            
            vt.addRow(row);
            this.titles.add(Pair.of(namespace, title));

            if (++batchSize % WikipediaConstants.BATCH_SIZE == 0) {
                this.loadVoltTable(catalog_tbl.getName(), vt);
                vt.clearRowData();
                batchSize = 0;
                if (LOG.isDebugEnabled()) {
                    int percent = (int) (((double) i / (double) this.num_pages) * 100);
                    if (percent != lastPercent) LOG.debug("PAGE (" + percent + "%)");
                    lastPercent = percent;
                }
            }
        } // FOR
        if (batchSize > 0) {
            this.loadVoltTable(catalog_tbl.getName(), vt);
            vt.clearRowData();
        }
        if (LOG.isDebugEnabled())
            LOG.debug("Users  % " + this.num_pages);
    }

    /**
     * WATCHLIST
     */
    private void loadWatchlist(Database catalog_db) {
        Table catalog_tbl = catalog_db.getTables().get(WikipediaConstants.TABLENAME_WATCHLIST);
        assert(catalog_tbl != null);
        
        Zipf h_numWatches = new Zipf(randGenerator, 0, this.num_pages, WikipediaConstants.NUM_WATCHES_PER_USER_SIGMA);
        Zipf h_pageId = new Zipf(randGenerator, 1, this.num_pages, WikipediaConstants.WATCHLIST_PAGE_SIGMA);

        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        int num_cols = catalog_tbl.getColumns().size();
        int batchSize = 0;
        int lastPercent = -1;
        Set<Integer> userPages = new HashSet<Integer>();
        for (int user_id = 1; user_id <= this.num_users; user_id++) {
            int num_watches = h_numWatches.nextInt();
            if (LOG.isTraceEnabled())
                LOG.trace(user_id + " => " + num_watches);
            
            userPages.clear();
            for (int i = 0; i < num_watches; i++) {
                int pageId = h_pageId.nextInt();
                while (userPages.contains(pageId)) {
                    pageId = h_pageId.nextInt();
                } // WHILE
                userPages.add(pageId);
                
                Pair<Integer, String> page = this.titles.get(pageId);
                assert(page != null) : "Invalid PageId " + pageId;
                
                Object row[] = new Object[num_cols];
                int param = 0;
                row[param++] = user_id; // wl_user
                row[param++] = page.getFirst(); // wl_namespace
                row[param++] = page.getSecond(); // wl_title
                row[param++] = null; // wl_notificationtimestamp
                vt.addRow(row);
                batchSize++;
            } // FOR

            if (batchSize >= WikipediaConstants.BATCH_SIZE) {
                //LOG.info("watchList(batch):\n" + vt);
                this.loadVoltTable(catalog_tbl.getName(), vt);
                vt.clearRowData();
                batchSize = 0;
                if (LOG.isDebugEnabled()) {
                    int percent = (int) (((double) user_id / (double) this.num_users) * 100);
                    if (percent != lastPercent) LOG.debug("WATCHLIST (" + percent + "%)");
                    lastPercent = percent;
                }
            }
        } // FOR
        if (batchSize > 0) {
            //LOG.info("watchList(<batch):\n" + vt);
            this.loadVoltTable(catalog_tbl.getName(), vt);
            vt.clearRowData();
        }
        if (LOG.isDebugEnabled())
            LOG.debug("Watchlist Loaded");
    }

    /**
     * REVISIONS
     */
    private void loadRevision(Database catalog_db) {
        
        // TEXT
        Table textTable = catalog_db.getTables().get(WikipediaConstants.TABLENAME_TEXT);
        assert(textTable != null) :
            "Failed to find " + WikipediaConstants.TABLENAME_TEXT;
        Column textTableColumn = textTable.getColumns().getIgnoreCase("OLD_TEXT");
        assert(textTableColumn != null) :
            "Failed to find " + WikipediaConstants.TABLENAME_TEXT + ".OLD_TEXT";
        int max_text_length = textTableColumn.getSize();
        
        // REVISION
        Table revTable = catalog_db.getTables().get(WikipediaConstants.TABLENAME_REVISION);
        assert(revTable != null) :
            "Failed to find " + WikipediaConstants.TABLENAME_REVISION;
        
        VoltTable vtText = CatalogUtil.getVoltTable(textTable);
        VoltTable vtRev = CatalogUtil.getVoltTable(revTable);
        int num_txt_cols = textTable.getColumns().size();
        int num_rev_cols = revTable.getColumns().size();
        int batchSize = 1;
        
        Zipf h_users = new Zipf(this.randGenerator, 1, this.num_users, WikipediaConstants.REVISION_USER_SIGMA);
        FlatHistogram<Integer> h_textLength = new FlatHistogram<Integer>(this.randGenerator, TextHistograms.TEXT_LENGTH);
        FlatHistogram<Integer> h_nameLength = new FlatHistogram<Integer>(this.randGenerator, UserHistograms.NAME_LENGTH);
        FlatHistogram<Integer> h_numRevisions = new FlatHistogram<Integer>(this.randGenerator, PageHistograms.REVISIONS_PER_PAGE);
        
        FlatHistogram<Integer> h_commentLength = new FlatHistogram<Integer>(this.randGenerator, RevisionHistograms.COMMENT_LENGTH);
        FlatHistogram<Integer> h_minorEdit = new FlatHistogram<Integer>(this.randGenerator, RevisionHistograms.MINOR_EDIT);
        
        int rev_id = 1;
        int lastPercent = -1;
        for (int page_id = 1; page_id <= this.num_pages; page_id++) {
            // There must be at least one revision per page
            int num_revised = h_numRevisions.nextValue().intValue();
            
            // Generate what the new revision is going to be
            int old_text_length = h_textLength.nextValue().intValue();
            //LOG.info("Max length:" + max_text_length + " old_text_length:" + old_text_length);
            assert(old_text_length > 0);
            assert(old_text_length < max_text_length);
            char old_text[] = TextGenerator.randomChars(randGenerator, old_text_length);
            
            TimestampType timestamp = new TimestampType();
            for (int i = 0; i < num_revised; i++) {
                // Generate the User who's doing the revision and the Page revised
                // Makes sure that we always update their counter
                int user_id = h_users.nextInt();
                assert(user_id > 0 && user_id <= this.num_users) : "Invalid UserId '" + user_id + "'";
                this.user_revision_ctr[user_id-1]++;
                
                // Generate what the new revision is going to be
                if (i > 0) {
                    old_text = WikipediaUtil.generateRevisionText(old_text);
                    old_text_length = old_text.length;
                }
                
                char rev_comment[] = TextGenerator.randomChars(randGenerator, h_commentLength.nextValue().intValue());

                // The REV_USER_TEXT field is usually the username, but we'll just 
                // put in gibberish for now
                char user_text[] = TextGenerator.randomChars(randGenerator, h_nameLength.nextValue().intValue());
                
                // Insert the text
                Object row[] = new Object[num_txt_cols];
                int col = 0;
                row[col++] = rev_id; // old_id
                row[col++] = new String(old_text); // old_text
                row[col++] = "utf-8"; // old_flags
                row[col++] = page_id; // old_page
                vtText.addRow(row);

                // Insert the revision
                col = 0;
                row = new Object[num_rev_cols];
                row[col++] = rev_id; // rev_id
                row[col++] = page_id; // rev_page
                row[col++] = rev_id; // rev_text_id
                row[col++] = new String(rev_comment); // rev_comment
                row[col++] = user_id; // rev_user
                row[col++] = new String(user_text); // rev_user_text
                row[col++] = timestamp; // rev_timestamp
                row[col++] = h_minorEdit.nextValue().intValue(); // rev_minor_edit
                row[col++] = 0; // rev_deleted
                row[col++] = 0; // rev_len
                row[col++] = 0; // rev_parent_id
                vtRev.addRow(row);
                
                // Update Last Revision Stuff
                this.page_last_rev_id[page_id-1] = rev_id;
                this.page_last_rev_length[page_id-1] = old_text_length;
                rev_id++;
                batchSize++;
            } // FOR (revision)
            if (batchSize >= WikipediaConstants.BATCH_SIZE) {
                LOG.info("Text Table(batch):\n" + vtText);
                LOG.info("Revision Table(batch):\n" + vtRev);
                
                this.loadVoltTable(textTable.getName(), vtText);
                this.loadVoltTable(revTable.getName(), vtRev);
                vtText.clearRowData();
                vtRev.clearRowData();
                batchSize = 0;
                
                if (LOG.isDebugEnabled()) {
                    int percent = (int) (((double) page_id / (double) this.num_pages) * 100);
                    if (percent != lastPercent) LOG.debug("REVISIONS (" + percent + "%)");
                    lastPercent = percent;
                }
            }
        } // FOR (page)
        
        if (batchSize > 0) {
            //LOG.info("Text Table(<batch):\n" + vtText);
            //LOG.info("Revision Table(<batch):\n" + vtRev);
            
            this.loadVoltTable(textTable.getName(), vtText);
            this.loadVoltTable(revTable.getName(), vtRev);
            vtText.clearRowData();
            vtRev.clearRowData();
            batchSize = 0;
        }
        
        
        // UPDATE USER & UPDATE PAGES
        batchSize = 0;
        Client client = this.getClientHandle();
        ClientResponse cr = null;
        try {
            cr = client.callProcedure(UpdateRevisionCounters.class.getSimpleName(),
                                      this.user_revision_ctr,
                                      this.num_pages,
                                      this.page_last_rev_id,
                                      this.page_last_rev_length);

        } catch (Exception ex) {
            throw new RuntimeException("Failed to update users and pages", ex);
        }
        assert(cr != null);
        assert(cr.getStatus() == Status.OK);
        
        if (LOG.isDebugEnabled()) {
            LOG.debug("Revision loaded");
        }
    }
   
}
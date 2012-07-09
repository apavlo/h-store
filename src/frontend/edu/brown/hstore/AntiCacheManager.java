package edu.brown.hstore;

import java.io.File;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.utils.FileUtil;

/**
 * A high-level manager for the anti-cache feature
 * Most of the work is done down in the EE, so this is just an abstraction 
 * layer for now
 * @author pavlo
 */
public class AntiCacheManager implements Shutdownable {

    private final HStoreSite hstore_site;
    private final Database catalog_db;
    private final HStoreConf hstore_conf;
    
    protected AntiCacheManager(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.catalog_db = hstore_site.getDatabase();
        this.hstore_conf = hstore_site.getHStoreConf();
    }
    
    /**
     * Queue the evicted blocks for a given Table to be read back in at this partition
     * This is a non-blocking call. The AntiCacheManager will figure out when it's
     * ready to get these blocks back in 
     * @param catalog_tbl
     * @param block_ids
     */
    public void queueReadBlocks(AbstractTransaction txn, int partition, Table catalog_tbl, short block_ids[]) {
        
    }
    
    @Override
    public void prepareShutdown(boolean error) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void shutdown() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public boolean isShuttingDown() {
        // TODO Auto-generated method stub
        return false;
    }
    
    // ----------------------------------------------------------------------------
    // STATIC HELPER METHODS
    // ----------------------------------------------------------------------------

    /**
     * Returns the directory where the EE should store the anti-cache
     * database for this PartitionExecutor
     * @return
     */
    public static File getDatabaseDir(PartitionExecutor executor) {
        HStoreConf hstore_conf = executor.getHStoreConf();
        Database catalog_db = CatalogUtil.getDatabase(executor.getCatalogSite());
        
        // First make sure that our base directory exists
        String base_dir = FileUtil.realpath(hstore_conf.site.anticache_dir + 
                                            File.separatorChar +
                                            catalog_db.getProject());
        synchronized (AntiCacheManager.class) {
            FileUtil.makeDirIfNotExists(base_dir);
        } // SYNCH
        
        // Then each partition will have a separate directory inside of the base one
        String partitionName = HStoreThreadManager.formatPartitionName(executor.getSiteId(),
                                                                       executor.getPartitionId());

        File dbDirPath = new File(base_dir + File.separatorChar + partitionName);
        FileUtil.makeDirIfNotExists(dbDirPath);
        
        // TODO: What do we do if the directory already exists?
        //       There should be an HStoreConf that says we should delete it first
        
        return (dbDirPath);
    }
    
}

package edu.brown.hstore;

import java.io.File;

import org.voltdb.catalog.Database;

import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.utils.FileUtil;

/**
 * A high-level manager for the anti-cache feature
 * Most of the work is done down in the EE, so this is just an abstraction 
 * layer for now
 * @author pavlo
 */
public class AntiCacheManager implements Shutdownable {

    private final PartitionExecutor executor;
    private final Database catalog_db;
    private final HStoreConf hstore_conf;
    
    protected AntiCacheManager(Database catalog_db, PartitionExecutor executor) {
        this.catalog_db = catalog_db;
        this.executor = executor;
        this.hstore_conf = this.executor.getHStoreConf();
    }
    
    /**
     * Returns the directory where the EE should store the anti-cache
     * database for this PartitionExecutor
     * @return
     */
    public File getDatabaseDir() {
        // First make sure that our base directory exists
        String base_dir = FileUtil.realpath(hstore_conf.site.anticache_dir + 
                                            File.separatorChar +
                                            this.catalog_db.getProject());
        synchronized (AntiCacheManager.class) {
            FileUtil.makeDirIfNotExists(base_dir);
        } // SYNCH
        
        // Then each partition will have a separate directory inside of the base one
        String partitionName = HStoreThreadManager.formatPartitionName(this.executor.getSiteId(),
                                                                       this.executor.getPartitionId());

        File dbDirPath = new File(base_dir + File.separatorChar + partitionName);
        FileUtil.makeDirIfNotExists(dbDirPath);
        
        // TODO: What do we do if the file already exists?
        //       There should be an HStoreConf that says we should delete it first
        
        return (dbDirPath);
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
    
    

}

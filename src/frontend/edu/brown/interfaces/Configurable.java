package edu.brown.interfaces;

import edu.brown.hstore.conf.HStoreConf;

/**
 * This interface is used for anything in the system that wants to be 
 * notified when the HStoreConf values change.
 * @author pavlo
 */
public interface Configurable {

    /**
     * Notification that some parameters in the HStoreConf have changed.
     * @param hstore_conf
     * @param changed The list of the parameters that have changed (will include prefix handle).
     */
    public void updateConf(HStoreConf hstore_conf, String[] changed);
    
}

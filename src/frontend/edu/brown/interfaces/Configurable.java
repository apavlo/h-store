package edu.brown.interfaces;

import edu.brown.hstore.conf.HStoreConf;

/**
 * This interface is used for anything in the system that wants to be 
 * notified when the HStoreConf values change.
 * @author pavlo
 */
public interface Configurable {

    public void updateConf(HStoreConf hstore_conf);
    
}

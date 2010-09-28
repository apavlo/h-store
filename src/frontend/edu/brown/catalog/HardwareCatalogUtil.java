package edu.brown.catalog;

import org.apache.log4j.Logger;
import org.voltdb.catalog.*;

public abstract class HardwareCatalogUtil {
    private static final Logger LOG = Logger.getLogger(HardwareCatalogUtil.class.getName());

    public static Host getHost(CatalogType catalog_item) {
        Host ret = null;
        if (catalog_item instanceof HardwareCPU ||
            catalog_item instanceof HardwareCore ||
            catalog_item instanceof HardwareThread) {
            ret = HardwareCatalogUtil.getHost(catalog_item.getParent());
        } else if (catalog_item instanceof Host) {
            ret = (Host)catalog_item;
        } else {
            assert(false) : "Unexpected catalog item " + catalog_item;
        }
        return (ret);
    }
    
    public static int getCoresPerCPU(CatalogType catalog_item) {
        Host catalog_host = HardwareCatalogUtil.getHost(catalog_item);
        if (catalog_host == null) {
            LOG.error("Could not retrieve Host in catalog for " + catalog_item);
            return (-1);
        }
        return (catalog_host.getCorespercpu());
    }
    
    public static int getThreadsPerCore(CatalogType catalog_item) {
        Host catalog_host = HardwareCatalogUtil.getHost(catalog_item);
        if (catalog_host == null) {
            LOG.error("Could not retrieve Host in catalog for " + catalog_item);
            return (-1);
        }
        return (catalog_host.getThreadspercore());
    }

}

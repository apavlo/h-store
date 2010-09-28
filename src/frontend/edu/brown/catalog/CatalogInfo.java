package edu.brown.catalog;

import edu.brown.utils.ArgumentsParser;

public class CatalogInfo {

    /**
     * @param args
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG);
        
        // Just print out the Host/Partition Information
        int num_hosts = CatalogUtil.getCluster(args.catalog).getHosts().size();
        int num_partitions = CatalogUtil.getNumberOfPartitions(args.catalog);
        
        System.out.println("Catalog File:    " + args.catalog_path.getName());
        System.out.println("# of Hosts:      " + num_hosts);
        System.out.println("# of Partitions: " + num_partitions);
    }

}

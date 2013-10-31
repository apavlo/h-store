/**
 * 
 */
package edu.brown.designer.mappers;

import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Table;

import edu.brown.designer.Designer;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.partitioners.plan.PartitionPlan;

/**
 * @author pavlo
 */
public class SimpleMapper extends AbstractMapper {

    public SimpleMapper(Designer designer, DesignerInfo info) {
        super(designer, info);
    }

    /*
     * (non-Javadoc)
     * @see
     * edu.brown.designer.mappers.AbstractMapper#generate(edu.brown.designer
     * .DesignerHints, edu.brown.designer.partitioners.PartitionPlan)
     */
    @Override
    public PartitionMapping generate(DesignerHints hints, PartitionPlan pplan) throws Exception {
        PartitionMapping pmap = new PartitionMapping();

        //
        // Sites
        //
        Cluster catalog_cluster = (Cluster) info.catalogContext.database.getParent();
        int site_id = 0;
        for (Host catalog_host : catalog_cluster.getHosts()) {
            int num_sites = catalog_host.getCorespercpu() * catalog_host.getThreadspercore();
            for (int ctr = 0; ctr < num_sites; ctr++) {
                SiteEntry site = new SiteEntry(site_id);
                pmap.assign(catalog_host, site);
                site_id++;
            } // FOR
        } // FOR

        //
        // Table Fragments
        //
        for (Table root : pplan.getNonReplicatedRoots()) {
            for (int ctr = 0; ctr < site_id; ctr++) {
                SiteEntry site = pmap.getSite(ctr);
                FragmentEntry fragment = new FragmentEntry(root, ctr);
                pmap.assign(site, fragment);
            } // FOR
        } // FOR
        pmap.initialize();
        return (pmap);
    }

}

package org.voltdb.benchmark.tpcc;

import org.voltdb.catalog.Database;

import edu.brown.statistics.AbstractTableStatisticsGenerator;
import edu.brown.utils.ProjectType;

/**
 * TPCCTableStatisticsGenerator
 * @author pavlo
 */
public class TPCCTableStatisticsGenerator extends AbstractTableStatisticsGenerator {

    public TPCCTableStatisticsGenerator(Database catalog_db, double scale_factor) {
        super(catalog_db, ProjectType.TPCC, scale_factor);
    }
    
    @Override
    public void createProfiles() {
        // TODO Auto-generated method stub
    }

}

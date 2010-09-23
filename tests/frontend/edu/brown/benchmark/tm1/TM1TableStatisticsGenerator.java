package edu.brown.benchmark.tm1;

import java.util.Map;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import edu.brown.statistics.AbstractTableStatisticsGenerator;
import edu.brown.statistics.TableStatistics;
import edu.brown.statistics.WorkloadStatistics;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.ProjectType;

/**
 * TM1 Initial Table Sizes
 * 
 * @author pavlo
 */
public class TM1TableStatisticsGenerator extends AbstractTableStatisticsGenerator {

    /**
     * TM1 
     */
    public static final long BASE_SUBSCRIBERS       = 100000;
    
    /**
     * @param catalogDb
     * @param projectType
     * @param scaleFactor
     */
    public TM1TableStatisticsGenerator(Database catalog_db, double scale_factor) {
        super(catalog_db, ProjectType.TM1, scale_factor);
    }

    @Override
    public void createProfiles() {
        TableProfile p = null;
        
        // SUBSCRIBER
        p = new TableProfile(this.catalog_db, TM1Constants.TABLENAME_SUBSCRIBER, false, BASE_SUBSCRIBERS);
        this.addTableProfile(p);
        
        // ACCESS_INFO
        p = new TableProfile(this.catalog_db, TM1Constants.TABLENAME_ACCESS_INFO, false);
        p.addAdditionDependency(this.catalog_db, TM1Constants.TABLENAME_SUBSCRIBER, 2.5);
        this.addTableProfile(p);

        // SPECIAL_FACILITY
        p = new TableProfile(this.catalog_db, TM1Constants.TABLENAME_SPECIAL_FACILITY, false);
        p.addAdditionDependency(this.catalog_db, TM1Constants.TABLENAME_SUBSCRIBER, 2.5);
        this.addTableProfile(p);

        // CALL_FORWARDING
        p = new TableProfile(this.catalog_db, TM1Constants.TABLENAME_CALL_FORWARDING, false);
        p.addAdditionDependency(this.catalog_db, TM1Constants.TABLENAME_SPECIAL_FACILITY, 1.5);
        this.addTableProfile(p);
    }
    
    /**
     * @param args
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
            ArgumentsParser.PARAM_CATALOG,
            ArgumentsParser.PARAM_STATS_OUTPUT,
            ArgumentsParser.PARAM_STATS_SCALE_FACTOR
        );
        
        double scale_factor = args.getDoubleParam(ArgumentsParser.PARAM_STATS_SCALE_FACTOR);
        Map<Table, TableStatistics> table_stats = new TM1TableStatisticsGenerator(args.catalog_db, scale_factor).generate();
        assert(table_stats != null);
        
        WorkloadStatistics stats = new WorkloadStatistics(args.catalog_db);
        stats.apply(table_stats);
        stats.save(args.getParam(ArgumentsParser.PARAM_STATS_OUTPUT));
    }

}
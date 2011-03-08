package edu.brown.benchmark.airline;

import java.io.File;

import org.voltdb.catalog.Catalog;

import edu.brown.BaseTestCase;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;

public abstract class AirlineBaseTestCase extends BaseTestCase {

    // HACK
    protected static String AIRLINE_DATA_DIR; //  = System.getenv(AirlineConstants.AIRLINE_DATA_PARAM.toUpperCase());
    protected static AirlineLoader loader;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.AIRLINE);
        if (loader == null) {
            File dir = FileUtil.findDirectory("tests");
            assertNotNull(dir);
            assert(dir.exists()) : "Missing " + dir.getAbsolutePath();
            File data_dir = new File(dir.getAbsolutePath() + "/frontend/" + AirlineBaseTestCase.class.getPackage().getName().replace(".", "/") + "/data");
            assert(data_dir.exists()) : "Missing " + data_dir.getAbsolutePath();
            AIRLINE_DATA_DIR = data_dir.getAbsolutePath();
            
            System.err.println(AirlineConstants.AIRLINE_DATA_PARAM + ": " + AIRLINE_DATA_DIR);
            
            final Catalog loader_catalog = catalog;
            loader = new AirlineLoader(
                new String[]{ AirlineConstants.AIRLINE_DATA_PARAM + "=" + AIRLINE_DATA_DIR }) {
                public Catalog getCatalog() throws Exception {
                    return (loader_catalog);
                };
            };
        }
    }
    
}

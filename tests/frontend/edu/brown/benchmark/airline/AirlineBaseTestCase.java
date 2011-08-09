package edu.brown.benchmark.airline;

import java.io.File;

import org.voltdb.catalog.Catalog;

import edu.brown.BaseTestCase;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;

public abstract class AirlineBaseTestCase extends BaseTestCase {

    // HACK
    protected static File AIRLINE_DATA_DIR; //  = System.getenv(AirlineConstants.AIRLINE_DATA_PARAM.toUpperCase());
    protected static AirlineLoader loader;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.AIRLINE);
        if (isFirstSetup()) {
            File dir = FileUtil.findDirectory("tests");
            assertNotNull(dir);
            assert(dir.exists()) : "Missing " + dir.getAbsolutePath();
            AIRLINE_DATA_DIR = new File(dir.getAbsolutePath() + "/frontend/" + AirlineBaseTestCase.class.getPackage().getName().replace(".", "/") + "/data");
            assert(AIRLINE_DATA_DIR.exists()) : "Missing " + AIRLINE_DATA_DIR.getAbsolutePath();
            
            final Catalog loader_catalog = catalog;
            loader = new AirlineLoader(
                new String[]{ "datadir" + "=" + AIRLINE_DATA_DIR }) {
                public Catalog getCatalog() {
                    return (loader_catalog);
                };
            };
        }
    }
    
}

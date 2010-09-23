package edu.brown.benchmark.airline;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public abstract class AirlineBaseTestCase extends BaseTestCase {

    // HACK
    protected static final String AIRLINE_DATA_DIR = System.getenv(AirlineConstants.AIRLINE_DATA_PARAM.toUpperCase());
    protected static AirlineLoader loader;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.AIRLINE);
        if (loader == null) {
            System.err.println(AirlineConstants.AIRLINE_DATA_PARAM + ": " + AIRLINE_DATA_DIR);
            loader = new AirlineLoader(new String[]{ AirlineConstants.AIRLINE_DATA_PARAM + "=" + AIRLINE_DATA_DIR });
        }
    }
    
}

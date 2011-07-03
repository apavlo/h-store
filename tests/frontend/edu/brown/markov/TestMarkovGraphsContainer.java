package edu.brown.markov;

import java.io.File;

import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;

public class TestMarkovGraphsContainer extends BaseTestCase {

    final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    Procedure catalog_proc;
    
    public void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(10);
        catalog_proc = this.getProcedure(TARGET_PROCEDURE);
    }
    
    public void testSerialization() throws Exception {
        MarkovGraphsContainer markovs = new MarkovGraphsContainer();
        for (Integer p : CatalogUtil.getAllPartitionIds(catalog_db)) {
            markovs.getOrCreate(p, catalog_proc, true);
        } // FOR
    
        // Serialize them out to a file. This will also make a nice little index in the file
        File temp = FileUtil.getTempFile("markovs", false);
        assertNotNull(temp);
        markovs.save(temp.getAbsolutePath());
        System.err.println("MARKOV FILE: " + temp);
    
        // Now read it back in make sure everything is there
        MarkovGraphsContainer clone = new MarkovGraphsContainer();
        clone.load(temp.getAbsolutePath(), catalog_db);
        assertNotNull(clone);
        assertEquals(markovs.size(), clone.size());
        assert(markovs.keySet().containsAll(clone.keySet()));
        for (Integer id : markovs.keySet()) {
            MarkovGraph clone_m = clone.get(id, catalog_proc);
            assertNotNull(clone_m);
        } // FOR
    }
    
}

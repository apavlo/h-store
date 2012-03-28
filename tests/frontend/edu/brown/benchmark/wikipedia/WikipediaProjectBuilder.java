/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package edu.brown.benchmark.wikipedia;

import org.apache.log4j.Logger;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.BenchmarkComponent;
import edu.brown.benchmark.wikipedia.procedures.AddWatchList;
import edu.brown.benchmark.wikipedia.procedures.GetPageAnonymous;
import edu.brown.benchmark.wikipedia.procedures.GetPageAuthenticated;
import edu.brown.benchmark.wikipedia.procedures.RemoveWatchList;
import edu.brown.benchmark.wikipedia.procedures.UpdatePage;

public class WikipediaProjectBuilder extends AbstractProjectBuilder {
    private static final Logger LOG = Logger.getLogger(WikipediaProjectBuilder.class);
    
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = WikipediaClient.class;
 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = WikipediaLoader.class;
 
    public static final Class<?> PROCEDURES[] = new Class<?>[] {
        AddWatchList.class,
        GetPageAnonymous.class,
        GetPageAuthenticated.class,
        RemoveWatchList.class,
        UpdatePage.class,
    };
    
    /**
     * FIXME how the schemas are partitioned...?
     */
    public static final String PARTITIONING[][] = new String[][] {
        // { "TABLE NAME", "PARTITIONING COLUMN NAME" }
       
    };
 
    public WikipediaProjectBuilder() {
        // TODO Auto-generated constructor stub
        super("Wikipedia", WikipediaProjectBuilder.class, PROCEDURES, PARTITIONING);
        
        // Create a single-statement stored procedure named 'DeleteData'
        addStmtProcedure("Test", "SELECT * FROM " + WikipediaConstants.TABLENAME_PAGE + 
                            " WHERE page_namespace = ? AND page_title = ? LIMIT 1");
  
    }
    
    public void addDefaultProcedures() {
        addProcedures(PROCEDURES);
    }
    
    public void addDefaultSchema() {
        
        addSchema(this.getDDLURL(true));
    }
	
//	@Override
//	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
//	    LOG.info(String.format("Initializing %d %s using '%s' as the input trace file",
//                               workConf.getTerminals(), WikipediaWorker.class.getSimpleName(), this.traceInput));
//		TransactionSelector transSel = new TransactionSelector(this.traceInput, workConf.getTransTypes());
//		List<WikipediaOperation> trace = Collections.unmodifiableList(transSel.readAll());
//		LOG.info("Total Number of Sample Operations: " + trace.size());
//		
//		ArrayList<Worker> workers = new ArrayList<Worker>();
//		for (int i = 0; i < workConf.getTerminals(); ++i) {
//			TransactionGenerator<WikipediaOperation> generator = new TraceTransactionGenerator(trace);
//			WikipediaWorker worker = new WikipediaWorker(i, this, generator);
//			workers.add(worker);
//		} // FOR
//		return workers;
//	}
//	@Override
//	protected Loader makeLoaderImpl(Connection conn) {
//		return new WikipediaLoader(this, conn);
//	}
}

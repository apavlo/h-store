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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

import edu.brown.benchmark.wikipedia.data.RevisionHistograms;
import edu.brown.benchmark.wikipedia.procedures.AddWatchList;
import edu.brown.benchmark.wikipedia.util.TraceTransactionGenerator;
import edu.brown.benchmark.wikipedia.util.TransactionSelector;
import edu.brown.benchmark.wikipedia.util.WikipediaOperation;
import edu.brown.benchmark.wikipedia.util.TextGenerator;

import edu.brown.WorkloadConfiguration;
import edu.brown.api.BenchmarkModule;
import edu.brown.api.TransactionGenerator;

public class WikipediaProjectBuilder extends BenchmarkModule {
    private static final Logger LOG = Logger.getLogger(WikipediaBenchmark.class);
    
	@SuppressWarnings("unchecked")
    public WikipediaBenchmark(WorkloadConfiguration workConf) {		
		super("wikipedia", workConf, true);
		
		this.commentLength = new FlatHistogram<Integer>(this.rng(), RevisionHistograms.COMMENT_LENGTH);
		this.minorEdit = new FlatHistogram<Integer>(this.rng(), RevisionHistograms.MINOR_EDIT);
		this.revisionDeltas = (FlatHistogram<Integer>[])new FlatHistogram[RevisionHistograms.REVISION_DELTA_SIZES.length];
		for (int i = 0; i < this.revisionDeltas.length; i++) {
		    this.revisionDeltas[i] = new FlatHistogram<Integer>(this.rng(), RevisionHistograms.REVISION_DELTAS[i]);
		} // FOR
	}

	
	/**
	 * 
	 * @param orig_text
	 * @return
	 */
	protected char[] generateRevisionText(char orig_text[]) {
	    // Figure out how much we are going to change
        // If the delta is greater than the length of the original
        // text, then we will just cut our length in half. Where is your god now?
        // There is probably some sort of minimal size that we should adhere to, but
        // it's 12:30am and I simply don't feel like dealing with that now
	    FlatHistogram<Integer> h = null;
	    for (int i = 0; i < this.revisionDeltas.length-1; i++) {
	        if (orig_text.length <= RevisionHistograms.REVISION_DELTA_SIZES[i]) {
	            h = this.revisionDeltas[i];
	        }
	    } // FOR
	    if (h == null) h = this.revisionDeltas[this.revisionDeltas.length-1];
	    assert(h != null);
	    
        int delta = h.nextValue().intValue();
        if (orig_text.length + delta <= 0) {
            delta = -1 * (int)Math.round(orig_text.length / 1.5);
            if (Math.abs(delta) == orig_text.length && delta < 0) delta /= 2;
        }
        if (delta != 0) orig_text = TextGenerator.resizeText(rng(), orig_text, delta);
        
        // And permute it a little bit. This ensures that the text is slightly
        // different than the last revision
        orig_text = TextGenerator.permuteText(rng(), orig_text);
        
        return (orig_text);
	}
	
	@Override
	protected Package getProcedurePackageImpl() {
		return (AddWatchList.class.getPackage());
	}
	
	@Override
	protected List<Worker> makeWorkersImpl(boolean verbose) throws IOException {
	    LOG.info(String.format("Initializing %d %s using '%s' as the input trace file",
                               workConf.getTerminals(), WikipediaWorker.class.getSimpleName(), this.traceInput));
		TransactionSelector transSel = new TransactionSelector(this.traceInput, workConf.getTransTypes());
		List<WikipediaOperation> trace = Collections.unmodifiableList(transSel.readAll());
		LOG.info("Total Number of Sample Operations: " + trace.size());
		
		ArrayList<Worker> workers = new ArrayList<Worker>();
		for (int i = 0; i < workConf.getTerminals(); ++i) {
			TransactionGenerator<WikipediaOperation> generator = new TraceTransactionGenerator(trace);
			WikipediaWorker worker = new WikipediaWorker(i, this, generator);
			workers.add(worker);
		} // FOR
		return workers;
	}
	
	@Override
	protected Loader makeLoaderImpl(Connection conn) throws SQLException {
		return new WikipediaLoader(this, conn);
	}
}

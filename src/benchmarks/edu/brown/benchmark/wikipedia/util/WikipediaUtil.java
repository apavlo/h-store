/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  Carlo Curino <carlo.curino@gmail.com>
 *                               Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch> 
 * 				Yang Zhang <yaaang@gmail.com> 
 * 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package edu.brown.benchmark.wikipedia.util;

import java.util.Random;

import org.apache.log4j.Logger;

import edu.brown.benchmark.wikipedia.data.RevisionHistograms;
import edu.brown.rand.RandomDistribution.FlatHistogram;


/** Immutable class containing information about transactions. */
public final class WikipediaUtil {
	
	private static final Logger LOG = Logger.getLogger(WikipediaUtil.class);
    
//    private static FlatHistogram<Integer> commentLength;
//    private static FlatHistogram<Integer> minorEdit;
	public static Random rand = new Random();
    public static  FlatHistogram<Integer> commentLength = new FlatHistogram<Integer>(rand, RevisionHistograms.COMMENT_LENGTH);
    public static FlatHistogram<Integer> minorEdit = new FlatHistogram<Integer>(rand, RevisionHistograms.MINOR_EDIT);
    
//    private static FlatHistogram<Integer> revisionDeltas[];

	public WikipediaUtil(int userId, int nameSpace, String pageTitle) {
		
//		this.commentLength = new FlatHistogram<Integer>(randGenerator, RevisionHistograms.COMMENT_LENGTH);
//        this.minorEdit = new FlatHistogram<Integer>(randGenerator, RevisionHistograms.MINOR_EDIT);
//        this.revisionDeltas = (FlatHistogram<Integer>[])new FlatHistogram [RevisionHistograms.REVISION_DELTA_SIZES.length];
//        for (int i = 0; i < revisionDeltas.length; i++) {
//            this.revisionDeltas[i] = new FlatHistogram<Integer>(randGenerator, RevisionHistograms.REVISION_DELTAS[i]);
//        } // FOR
	}
	
	public String toString() {
	    return String.format("<UserId:%d, NameSpace:%d, Title:%s>");
	    
	}
	
	/**
     * 
     * @param orig_text
     * @return
     */
    public static char[] generateRevisionText(char orig_text[]) {
        Random randGenerator = new Random();
       
        FlatHistogram<Integer> revisionDeltas[] = (FlatHistogram<Integer>[])new FlatHistogram [RevisionHistograms.REVISION_DELTA_SIZES.length];
        
        for (int i = 0; i < revisionDeltas.length; i++) {
            revisionDeltas[i] = new FlatHistogram<Integer>(randGenerator, RevisionHistograms.REVISION_DELTAS[i]);
        } // FOR
        // Figure out how much we are going to change
        // If the delta is greater than the length of the original
        // text, then we will just cut our length in half. Where is your god now?
        // There is probably some sort of minimal size that we should adhere to, but
        // it's 12:30am and I simply don't feel like dealing with that now
        FlatHistogram<Integer> h = null;
        for (int i = 0; i < revisionDeltas.length-1; i++) {
            if (orig_text.length <= RevisionHistograms.REVISION_DELTA_SIZES[i]) {
                h = revisionDeltas[i];
            }
        } // FOR
        if (h == null) h = revisionDeltas[revisionDeltas.length-1];
        assert(h != null);
        
        LOG.debug("");
        
        int delta = h.nextValue().intValue();
        if (orig_text.length + delta <= 0) {
            delta = -1 * (int)Math.round(orig_text.length / 1.5);
            if (Math.abs(delta) == orig_text.length && delta < 0) delta /= 2;
        }
        if (delta != 0) orig_text = TextGenerator.resizeText(randGenerator, orig_text, delta);
        
        // And permute it a little bit. This ensures that the text is slightly
        // different than the last revision
        orig_text = TextGenerator.permuteText(randGenerator, orig_text);
        
        return (orig_text);
    }
	
}

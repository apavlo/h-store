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

import edu.brown.benchmark.wikipedia.WikipediaConstants;
import edu.brown.benchmark.wikipedia.data.PageHistograms;
import edu.brown.benchmark.wikipedia.data.RevisionHistograms;
import edu.brown.benchmark.wikipedia.data.UserHistograms;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.rand.RandomDistribution.Zipf;

/**
 * Helper class that contains all of the useful information about the benchmark database
 * @author pavlo
 */
public final class WikipediaUtil {
	
    private final Random rand;
    private final double scaleFactor;
    
    public final int num_users;
    public final int num_pages;
    
    public final FlatHistogram<Integer> h_nameLength;
    public final FlatHistogram<Integer> h_realNameLength;
    public final FlatHistogram<Integer> h_revCount;
    public final FlatHistogram<Integer> h_titleLength;
    public final FlatHistogram<String> h_restrictions;
    public final Zipf h_watchPageCount;
    public final Zipf h_watchPageId;
    public final FlatHistogram<Integer> h_commentLength;
    public final FlatHistogram<Integer> h_minorEdit;

    /**
     * Constructor
     * @param rand
     * @param scaleFactor
     */
    public WikipediaUtil(Random rand, double scaleFactor) {
        this.rand = rand;
        this.scaleFactor = scaleFactor;
        
        this.num_users = (int) Math.round(WikipediaConstants.USERS * this.scaleFactor);
        this.num_pages = (int) Math.round(WikipediaConstants.PAGES * this.scaleFactor);
        
        this.h_nameLength = new FlatHistogram<Integer>(this.rand, UserHistograms.NAME_LENGTH);
        this.h_realNameLength = new FlatHistogram<Integer>(this.rand, UserHistograms.REAL_NAME_LENGTH);
        this.h_revCount = new FlatHistogram<Integer>(this.rand, UserHistograms.REVISION_COUNT);
        this.h_titleLength = new FlatHistogram<Integer>(this.rand, PageHistograms.TITLE_LENGTH);
        this.h_restrictions = new FlatHistogram<String>(this.rand, PageHistograms.RESTRICTIONS);
        this.h_watchPageCount = new Zipf(this.rand, 0, this.num_pages, WikipediaConstants.NUM_WATCHES_PER_USER_SIGMA);
        this.h_watchPageId = new Zipf(this.rand, 1, this.num_pages, WikipediaConstants.WATCHLIST_PAGE_SIGMA);
        this.h_commentLength = new FlatHistogram<Integer>(this.rand, RevisionHistograms.COMMENT_LENGTH);
        this.h_minorEdit = new FlatHistogram<Integer>(this.rand, RevisionHistograms.MINOR_EDIT);
    }
	
    /**
     * Return the computed namespace for the given pageId
     * @param pageId
     * @return
     */
    public int getPageNameSpace(long pageId) {
        return (0);
    }
    
	
	/**
     * 
     * @param orig_text
     * @return
     */
    public char[] generateRevisionText(char orig_text[]) {
        Random randGenerator = new Random();
       
        @SuppressWarnings("unchecked")
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

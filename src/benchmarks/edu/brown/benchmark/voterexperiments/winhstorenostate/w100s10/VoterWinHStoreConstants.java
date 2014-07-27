/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original By: VoltDB Inc.											   *
 *  Ported By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)  *                                                                      *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/

package edu.brown.benchmark.voterexperiments.winhstorenostate.w100s10;

public abstract class VoterWinHStoreConstants {

    public static final String TABLENAME_CONTESTANTS     = "contestants";
    public static final String TABLENAME_AREA_CODE_STATE = "area_code_state";
    public static final String TABLENAME_VOTES           = "votes";
    
	public static final int MAX_VOTES = 1000; 
	public static final int NUM_CONTESTANTS = 6; 

	// Initialize some common constants and variables
    public static final String CONTESTANT_NAMES_CSV = "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway," +
											   "Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster," +
											   "Kurt Walser,Ericka Dieter,Loraine NygrenTania Mattioli";
    // potential return codes
    public static final long VOTE_SUCCESSFUL = 0;
    public static final long ERR_INVALID_CONTESTANT = 1;
    public static final long ERR_VOTER_OVER_VOTE_LIMIT = 2;
    
    //public static final long WINDOW_SIZE = MicroWinSStoreConstants.WINDOW_SIZE;
    //public static final long SLIDE_SIZE = MicroWinSStoreConstants.SLIDE_SIZE;
    
    public static final long WINDOW_SIZE = 100;
    public static final long SLIDE_SIZE = 10;
}

/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original By: VoltDB Inc.											   *
 *  Ported By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)  *								   
 *                                                                         *
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

package edu.brown.benchmark.voter;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class VoterLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(VoterLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + VoterLoader.class.getName());
        Loader.main(VoterLoader.class, args, true);
    }

    public VoterLoader(String[] args) {
        super(args);
        if (d) LOG.debug("CONSTRUCTOR: " + VoterLoader.class.getName());
    }

    @Override
    public void load() {
        int numContestants = VoterUtil.getScaledNumContestants(this.getScaleFactor());
        if (d) 
            LOG.debug("Starting VoterLoader [numContestants=" + numContestants + "]");

        try {
            this.getClientHandle().callProcedure("Initialize",
                                                 numContestants,
                                                 VoterConstants.CONTESTANT_NAMES_CSV);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

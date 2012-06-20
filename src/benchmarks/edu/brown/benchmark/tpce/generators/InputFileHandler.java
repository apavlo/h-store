/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Alex Kalinin (akalinin@cs.brown.edu)                                   *
 *  http://www.cs.brown.edu/~akalinin/                                     *
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

package edu.brown.benchmark.tpce.generators;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * @author akalinin
 * 
 * This interface describes all types of files from the TPC-E benchmark.
 * 
 * The files are somewhat different, so it probably should be refactored later.
 *
 */
public abstract class InputFileHandler {
    private static final Logger LOG = Logger.getLogger(InputFileHandler.class);
    private BufferedReader reader;
    private final File inputFile;
    
    public InputFileHandler(File inputFile) {
        this.inputFile = inputFile;
    }
    
    public void parseFile() {
        try {
            reader = new BufferedReader(new FileReader(inputFile));
            String tupleString = reader.readLine();
            
            while (tupleString != null && !tupleString.equals("")) {
                insertTuple(tupleString.split("\t"));
                tupleString = reader.readLine();
            }
            
            LOG.trace("Input file parsed: '" + inputFile + "', maxKey = " + getMaxKey() + ", RecordsNum = " + getRecordsNum()); 
        }
        catch (FileNotFoundException e) {
            LOG.error("Unable to start benchmark. Missing '" + inputFile.toString() + "' input file");
            System.exit(1);
        }
        catch (IOException e) {
            LOG.error("Unable to read input file '" + inputFile.toString() + "'");
            System.exit(1);
        }
    }
    
    /*
     * Inserts a tuple into in-memory structure
     */
    protected abstract void insertTuple(String[] tuple);
    
    /*
     * Returns a tuple by ordinal number
     * 
     * The id here is usually strictly defined. See the next function in contrast.
     */
    public abstract String[] getTupleByIndex(int index);
    
    /*
     * Returns a list of tuples by index.
     * 
     * The index here is strictly defined. A list of tuples is returned
     */
    public abstract List<String[]> getTuplesByIndex(int index);
    
    /*
     * Returns a tuple by key
     * 
     * That means the weights (e.g., StreetName, ZipCode, etc.) come into play.
     * Usually it is used when the key is randomly generated.
     * 
     *  By default, weights are assumed to be 1.
     */
    public String[] getTupleByKey(int key) {
        return getTupleByIndex(key);
    }
    
    /*
     * Returns the number of records (tuple or lists of tuples).
     * 
     * Can be used to determine the maximum index number
     */
    public abstract int getRecordsNum();
    
    /*
     * Returns the maximum key number
     * 
     * Used for specifying the maximum in random functions
     */
    public int getMaxKey() {
        return getRecordsNum() - 1;
    }
}

/***************************************************************************
 *  Copyright (C) 2013 by H-Store Project                                  *
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

package edu.brown.benchmark.smallbank;

public abstract class SmallBankConstants {

    // ----------------------------------------------------------------
    // STORED PROCEDURE EXECUTION FREQUENCIES (0-100)
    // ----------------------------------------------------------------
    public static final int FREQUENCY_AMALGAMATE        = 20;
    public static final int FREQUENCY_BALANCE           = 20;
    public static final int FREQUENCY_DEPOSIT_CHECKING  = 20;
    public static final int FREQUENCY_SEND_PAYMENT      = 0;
    public static final int FREQUENCY_TRANSACT_SAVINGS  = 20;
    public static final int FREQUENCY_WRITE_CHECK       = 20;

    // ----------------------------------------------------------------
    // TABLE NAMES
    // ----------------------------------------------------------------
    public static final String TABLENAME_ACCOUNTS   = "ACCOUNTS";
    public static final String TABLENAME_SAVINGS    = "SAVINGS";
    public static final String TABLENAME_CHECKING   = "CHECKING";
    
    public static final int BATCH_SIZE              = 1000;
    
    // ----------------------------------------------------------------
    // ACCOUNT INFORMATION
    // ----------------------------------------------------------------
    
    // Default number of customers in bank
    public static final int NUM_ACCOUNTS            = 1000000;
    
    public static final int HOTSPOT_SIZE            = 100;
    
    // ----------------------------------------------------------------
    // ADDITIONAL CONFIGURATION SETTINGS
    // ----------------------------------------------------------------
    
    // Percentage of customers that do not have a SAVINGS account [0-100%]
    public static final int PERCENTAGE_NO_SAVINGS   = 0;
    
    // Percentage of customers that do not have a CHECKING account [0-100%]
    public static final int PERCENTAGE_NO_CHECKING  = 0;
    
    // Initial balance amount
    public static final int MIN_BALANCE             = 100;
    public static final int MAX_BALANCE             = 5000;

}

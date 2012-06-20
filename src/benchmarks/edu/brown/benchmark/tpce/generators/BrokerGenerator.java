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

import org.voltdb.catalog.Table;

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.generators.StatusTypeGenerator.StatusTypeId;
import edu.brown.benchmark.tpce.generators.TPCEGenerator.InputFile;
import edu.brown.benchmark.tpce.util.EGenRandom;

/**
 * @author akalinin
 *
 */
public class BrokerGenerator extends TableGenerator {
    
    // starting ID to generate names from for brokers
    private static final long BROKER_NAME_ID_SHIFT = 1000 * 1000;
    
    private long totalBrokers;
    private long counter;
    private long startFromBroker;
    
    private final PersonHandler personHandler;
    private final InputFileHandler statusTypeFile;
    
    private int[] numTrades = null;
    private double[] commTotal = null;

    /**
     * @param catalog_tbl
     * @param generator
     */
    public BrokerGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        totalBrokers = generator.getCustomersNum() / TPCEConstants.BROKERS_DIV;
        startFromBroker = generator.getStartCustomer() / TPCEConstants.BROKERS_DIV +
                TPCEConstants.STARTING_BROKER_ID + TPCEConstants.IDENT_SHIFT;
        
        personHandler = new PersonHandler(generator.getInputFile(InputFile.LNAME), generator.getInputFile(InputFile.FEMFNAME),
                generator.getInputFile(InputFile.MALEFNAME));
        statusTypeFile = generator.getInputFile(InputFile.STATUS);
    }
    
    public void initNextLoadUnit(long customerNum, long startCustomer) {
        // initial initialization
        if (totalBrokers != customerNum / TPCEConstants.BROKERS_DIV || numTrades == null || commTotal == null) {
            totalBrokers = customerNum / TPCEConstants.BROKERS_DIV;
            
            numTrades = new int[(int)totalBrokers];
            commTotal = new double[(int)totalBrokers];
        }
        
        // zero the arrays
        for (int i = 0; i < totalBrokers; i++) {
            numTrades[i] = 0;
            commTotal[i] = 0;
        }
        
        startFromBroker = (startCustomer / TPCEConstants.BROKERS_DIV) + TPCEConstants.STARTING_BROKER_ID + TPCEConstants.IDENT_SHIFT;
        counter = 0;       
    }
    
    public void updateTradeAndCommissionYTD(long brokerId, int tradeIncrement, double commissionIncrement) {
        if (brokerId >= startFromBroker && brokerId < (startFromBroker + totalBrokers)) {
            numTrades[(int)(brokerId - startFromBroker)] += tradeIncrement;
            commTotal[(int)(brokerId - startFromBroker)] += commissionIncrement;
        }
    }
    
    public long GenerateRandomBrokerId(EGenRandom  rnd)
	{
	    return rnd.int64Range(startFromBroker, startFromBroker + totalBrokers - 1);
	}
    
    public String generateBrokerName(long brokerId) {
        return personHandler.getFirstName(brokerId + BROKER_NAME_ID_SHIFT) + " " +
                personHandler.getMiddleName(brokerId + BROKER_NAME_ID_SHIFT) + " " +
                personHandler.getLastName(brokerId + BROKER_NAME_ID_SHIFT);
    }

    public long GetBrokerCount()
	{
	    return totalBrokers;
	}
    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return counter < totalBrokers;
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    @Override
    public Object[] next() {
        Object[] tuple = new Object[columnsNum];
        long brokerId = startFromBroker + counter;
        
        tuple[0] = brokerId; // b_id
        tuple[1] = statusTypeFile.getTupleByIndex(StatusTypeId.E_ACTIVE.ordinal())[0]; // b_st_id
        tuple[2] = generateBrokerName(brokerId); // b_name
        tuple[3] = numTrades[(int)(brokerId - startFromBroker)]; // b_num_trades
        tuple[4] = commTotal[(int)(brokerId - startFromBroker)]; // b_comm_total
        
        counter++;
        
        return tuple;
    }
}

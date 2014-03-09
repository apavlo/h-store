package edu.brown.benchmark.tpceb.generators;


import org.voltdb.catalog.Table;

import edu.brown.benchmark.tpceb.TPCEConstants;
import edu.brown.benchmark.tpceb.generators.TPCEGenerator.InputFile;
import edu.brown.benchmark.tpceb.util.EGenRandom;

/**
 * @author akalinin
 *
 */
public class NewBrokerGenerator extends TableGenerator {
    
    // starting ID to generate names from for brokers
    private static final long BROKER_NAME_ID_SHIFT = 1000 * 1000;
    private int tradeIncrement =10;
    private int commissionIncrement =10;
    private long totalBrokers;
    private long counter;
    private long startFromBroker;
    
    private int[] numTrades = null;
    private double[] commTotal = null;

    /**
     * @param catalog_tbl
     * @param generator
     */
    public NewBrokerGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        totalBrokers = generator.getCustomersNum() / TPCEConstants.BROKERS_DIV;
        startFromBroker = generator.getStartCustomer() / TPCEConstants.BROKERS_DIV +
                TPCEConstants.STARTING_BROKER_ID + TPCEConstants.IDENT_SHIFT;
   
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
    
    public void updateTradeAndCommissionYTD(long brokerId) {
        if (brokerId >= startFromBroker && brokerId < (startFromBroker + totalBrokers)) {
            numTrades[(int)(brokerId - startFromBroker)] += tradeIncrement;
            commTotal[(int)(brokerId - startFromBroker)] += commissionIncrement;
        }
    }
    
    public void setTradeIncrement(int newInc){
        tradeIncrement = newInc;
    }
    
    public void setcommssionIncrement(int newInc){
        commissionIncrement = newInc;
    }
    
    public int getTradeIncrement(){
        return tradeIncrement;
    }
    
    public int getcommissionIncrement(int newInc){
        return commissionIncrement;
    }
    
    public long GenerateRandomBrokerId(EGenRandom  rnd)
    {
        return rnd.int64Range(startFromBroker, startFromBroker + totalBrokers - 1);
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
        tuple[1] = numTrades[(int)(brokerId - startFromBroker)]; // b_num_trades
        tuple[2] = commTotal[(int)(brokerId - startFromBroker)]; // b_comm_total
        System.out.println("here");
        
        counter++;
        
        return tuple;
    }
}

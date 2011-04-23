package edu.brown.benchmark.tm1;

import java.util.Random;

import org.voltdb.benchmark.ClientMain;
import org.voltdb.client.Client;

public abstract class TM1BaseClient extends ClientMain {

    protected double scaleFactor = 1.0;
    protected long subscriberSize = 100000l;
    protected final Random rand = new Random();
    
    /**
     * When 
     */
    protected final boolean blocking = true;
    
    
    public TM1BaseClient(Client client) {
        super(client);
        // TODO Auto-generated constructor stub
    }

    public TM1BaseClient(String[] args) {
        super(args);
        
        for (String arg : args) {
            String[] parts = arg.split("=",2);
            if (parts.length == 1)
                continue;
            if (parts[1].startsWith("${"))
                continue;
            if (parts[0].equalsIgnoreCase("scalefactor")) {
                scaleFactor = Double.parseDouble(parts[1]);
                subscriberSize = (int)(subscriberSize / scaleFactor);
            }
        } // FOR
        
    }
    
    public long getSubscriberSize() {
        return subscriberSize;
    }
    
    public double getScaleFactor() {
        return scaleFactor;
    }

}

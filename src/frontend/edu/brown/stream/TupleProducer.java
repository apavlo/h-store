package edu.brown.stream;
import java.util.concurrent.BlockingQueue;

import edu.brown.utils.ThreadUtil;

public class TupleProducer implements Runnable {
    
    private BlockingQueue<Tuple> queue;
    private long fixnum;
     
    public TupleProducer(BlockingQueue<Tuple> q, long number){
        this.queue = q;
        this.fixnum = number;
    }
    @Override
    public void run() {
        //produce tuples
        try {
            rateControlledRunLoop();
        } catch (Throwable ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        } 
        finally 
        {
            Tuple tuple = null;
            //adding exit message
            tuple = new Tuple("exit");
            
            try 
            {
                queue.put(tuple);
            } 
            catch (InterruptedException e) 
            {
                e.printStackTrace();
            }
        }
    }
    
    private void rateControlledRunLoop() throws Exception {
        
        int transactionRate=1000;
        double txnsPerMillisecond = transactionRate / 1000.0;
        
        long lastRequestTime = System.currentTimeMillis();
        
        boolean hadErrors = false;

        Tuple tuple = null;
        
        long counter = 0;
        boolean beStop = false;
        
        while (true) {
            final long now = System.currentTimeMillis();
            final long delta = now - lastRequestTime;
            if (delta > 0) {
                final int transactionsToCreate = (int) (delta * txnsPerMillisecond);
                if (transactionsToCreate < 1) {
                    Thread.sleep(25);
                    continue;
                }

                try {
                    for (int ii = 0; ii < transactionsToCreate; ii++) 
                    {
                        tuple = new Tuple("" + counter);
                        queue.put(tuple);
                        counter++;
                        //System.out.println("Produced: "+ Long.toString(counter));
                        
                        if(counter == this.fixnum)
                        {
                            beStop = true;
                            break;
                        }
                    } 
                } catch (final Exception e) {
                    if (hadErrors) return;
                    hadErrors = true;
                    ThreadUtil.sleep(5000);
                } finally {
                }
                
                if( beStop == true )
                {
                    break;
                }
            }
            else {
                Thread.sleep(25);
            }

            lastRequestTime = now;
            
        } // WHILE
    }
 
}

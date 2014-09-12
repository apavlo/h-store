package edu.brown.stream.SparkBatchVoter;

import java.util.concurrent.BlockingQueue;

import edu.brown.stream.Tuple;
import edu.brown.stream.PhoneCallGenerator.PhoneCall;
import edu.brown.stream.VoteGenerator;
import edu.brown.utils.ThreadUtil;

public class VoteProducer implements Runnable {
    
    private BlockingQueue<Tuple> queue;
    //private long fixnum;
    private int sendrate;
    private boolean sendstop;
    private boolean stop = false;

    private VoteGenerator voteGenerator;
     
    public VoteProducer(BlockingQueue<Tuple> q, String filename, int sendrate, boolean sendstop){
        this.queue = q;
        this.sendrate = sendrate;
        this.sendstop = sendstop;
        
        this.voteGenerator = new VoteGenerator( filename );
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
            tuple = new Tuple();
            
            try 
            {
                queue.put(tuple);
                //System.out.println("Info - TupleProducer :" + "put empty tuple to indicate end" );
            } 
            catch (InterruptedException e) 
            {
                e.printStackTrace();
            }
        }
    }
    
    private void rateControlledRunLoop() throws Exception {
        
        int transactionRate= this.sendrate;
        
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
                        if(stop==true)
                        {
                            beStop = true;
                            break;
                        }
                        
                        if(this.voteGenerator.hasMoreVotes()==true)
                            //if(this.wordGenerator.hasMoreWords()==true)
                        {
                            // create tuple
                            tuple = new Tuple();
                            PhoneCall call = this.voteGenerator.nextVote();
                            tuple.addField("ID",call.voteId);
                            tuple.addField("CONTESTANT",call.contestantNumber);
                            tuple.addField("PHONENUMBER",call.phoneNumber);
                            tuple.addField("TIMESTAMP",call.timestamp);
                        }
                        else
                        {
                            if (sendstop==true)
                            {
                              System.out.println("Info - VoteProducer :" + "finish sending #votes - " + (counter));
                              //queue.put(null);
                              beStop = true;
                              System.out.println("beStop : " + beStop);
                              break;
                            }
                            else
                            {
                                voteGenerator.reset();
                                // create tuple
                                tuple = new Tuple();
                                PhoneCall call = this.voteGenerator.nextVote();
                                tuple.addField("ID",call.voteId);
                                tuple.addField("CONTESTANT",call.contestantNumber);
                                tuple.addField("PHONENUMBER",call.phoneNumber);
                                tuple.addField("TIMESTAMP",call.timestamp);
                            }
                        }
                        
                        //System.out.println("Vote: "+ tuple.toJSONString());
                        queue.put(tuple);
                        counter++;
                        //System.out.println("Produced: "+ Long.toString(counter));
                        
//                        if(counter == this.fixnum)
//                        {
//                            System.out.println("Info - TupleProducer :" + "finish sending #tuples - " + this.fixnum );
//                            beStop = true;
//                            break;
//                        }
                    } 
                } catch (final Exception e) {
                    if (hadErrors) return;
                    hadErrors = true;
                    ThreadUtil.sleep(5000);
                } finally {
                }
                
                if( beStop == true )
                {
                    queue.clear();
                    System.out.println("Finish sending votes!");
                    break;
                }
            }
            else {
                Thread.sleep(25);
            }

            lastRequestTime = now;
            
        } // WHILE
    }

    public void stop() {
        this.stop = true;
    }
 
}

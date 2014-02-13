package edu.brown.benchmark.wordcountsstorewithbatch;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.stream.Batch;
import edu.brown.stream.Tuple;
import edu.brown.benchmark.wordcountsstorewithbatch.procedures.SimpleCall;

public class WordCountSStoreWithBatchClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(WordCountSStoreWithBatchClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static long lastTime;
    private static int timestamp;
    
    // word generator
    WordGenerator wordGenerator;

    final Callback callback = new Callback();

    public static void main(String args[]) {
        BenchmarkComponent.main(WordCountSStoreWithBatchClient.class, args, false);
    }

    public WordCountSStoreWithBatchClient(String args[]) {
        super(args);
        String strFileName = "word.txt";
        
        this.wordGenerator = new WordGenerator(this.getClientId(), strFileName);
        lastTime = System.nanoTime();
        timestamp = 0;
    }

    @Override
    public void runLoop() {
        while (true) {
            try {
                runOnce();
            } catch (Exception e) {
            e.printStackTrace();
            }
        }
    }

    @Override
    protected boolean runOnce() throws IOException {
        String word;
        if(wordGenerator.isEmpty()==false)
        {
            if(wordGenerator.hasMoreWords()==false)
                wordGenerator.reset();
            
            word = wordGenerator.nextWord();
            boolean response = false;
            Client client = this.getClientHandle();
            
            long currentTime = System.nanoTime();
            if(currentTime - lastTime >= 1000000000)
            {
            	lastTime = System.nanoTime();
            	timestamp++;
            }
            
            // create batch
            Batch batch = new Batch();
            batch.setID(timestamp);
            batch.setTimestamp(timestamp);
            Tuple tuple = new Tuple();
            tuple.addField("WORD", word);
            tuple.addField("TIMESTAMP", currentTime);
            batch.addTuple(tuple);
            
	        response = client.callProcedure(callback, "SimpleCall", batch.toJSONString());

            return response;
        }
        else
            return false;
    }

    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[]{
             SimpleCall.class.getSimpleName()
        };
        return (procNames);
    }

    private class Callback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, 0);
        }
    } // END CLASS
}


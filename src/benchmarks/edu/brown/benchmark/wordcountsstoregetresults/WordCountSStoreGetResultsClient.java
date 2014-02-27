package edu.brown.benchmark.wordcountsstoregetresults;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.stream.Batch;
import edu.brown.benchmark.wordcountsstoregetresults.procedures.SimpleCall;

public class WordCountSStoreGetResultsClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(WordCountSStoreGetResultsClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static long lastTime;
    private static long getBatch;
    private static int timestamp;
    private static boolean firstRun;
    
    // word generator
    WordGenerator wordGenerator;

    final Callback callback = new Callback();

    public static void main(String args[]) {
        BenchmarkComponent.main(WordCountSStoreGetResultsClient.class, args, false);
    }

    public WordCountSStoreGetResultsClient(String args[]) {
        super(args);
        String strFileName = "word.txt";
        
        this.wordGenerator = new WordGenerator(this.getClientId(), strFileName);
        firstRun = true;
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
        if(firstRun)
        {
        	lastTime = System.nanoTime();
        	getBatch = System.nanoTime() - 500000000L;
            timestamp = 0;
            firstRun = false;
        }
        
        if(wordGenerator.isEmpty()==false)
        {
            if(wordGenerator.hasMoreWords()==false)
                wordGenerator.reset();
            
            
            boolean response = false;
            Client client = this.getClientHandle();
            if(System.nanoTime() - getBatch >= 1000000000)
            {
            	getBatch = System.nanoTime();
            	return client.callProcedure(callback, "GetResults");
            }
            
            if(System.nanoTime() - lastTime >= 1000000000)
            {
            	lastTime = System.nanoTime();
            	timestamp++;
            	//client.callProcedure(callback, "GetResults");
            }

            word = wordGenerator.nextWord();
            response = client.callProcedure(callback, "SimpleCall", word, timestamp);

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


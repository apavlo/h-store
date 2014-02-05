package edu.brown.benchmark.wordcount;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.benchmark.wordcount.procedures.SimpleCall;

public class WordCountClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(WordCountClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    
    // word generator
    WordGenerator wordGenerator;

    final Callback callback = new Callback();

    public static void main(String args[]) {
        BenchmarkComponent.main(WordCountClient.class, args, false);
    }

    public WordCountClient(String args[]) {
        super(args);
        String strFileName = "word.txt";
        
        this.wordGenerator = new WordGenerator(this.getClientId(), strFileName);
    }

    @Override
    public void runLoop() {
        try {
            while (true) {
                try {
                    runOnce();
                } catch (Exception e) {
	            e.printStackTrace();
                }

            } // WHILE
        } catch (Exception e) {
            // Client has no clean mechanism for terminating with the DB.
            e.printStackTrace();
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

            Client client = this.getClientHandle();
            boolean response = client.callProcedure(callback,
                                                    "SimpleCall", word);
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


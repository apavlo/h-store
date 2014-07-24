package edu.brown.benchmark.recovery;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;
import edu.brown.benchmark.voterdemosstore.VoterDemoSStoreConstants;

public class RecoveryLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(RecoveryLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + RecoveryLoader.class.getName());
        Loader.main(RecoveryLoader.class, args, true);
    }

    public RecoveryLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
        try {
            for (int i=0; i<=5; i++)
                this.getClientHandle().callProcedure("SimpleCall", i);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}


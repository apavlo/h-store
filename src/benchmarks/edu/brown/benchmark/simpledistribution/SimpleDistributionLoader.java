package edu.brown.benchmark.simpledistribution;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;
import edu.brown.benchmark.voter.VoterConstants;

public class SimpleDistributionLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(SimpleDistributionLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + SimpleDistributionLoader.class.getName());
        Loader.main(SimpleDistributionLoader.class, args, true);
    }

    public SimpleDistributionLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
        try {
            this.getClientHandle().callProcedure("Initialize");
        } catch (Exception e) {  
            throw new RuntimeException(e);
        }
    }
}


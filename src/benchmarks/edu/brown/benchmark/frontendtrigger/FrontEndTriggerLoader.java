package edu.brown.benchmark.frontendtrigger;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class FrontEndTriggerLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(FrontEndTriggerLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + FrontEndTriggerLoader.class.getName());
        Loader.main(FrontEndTriggerLoader.class, args, true);
    }

    public FrontEndTriggerLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


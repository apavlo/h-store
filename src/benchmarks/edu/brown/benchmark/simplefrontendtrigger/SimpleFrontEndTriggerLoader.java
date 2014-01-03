package edu.brown.benchmark.simplefrontendtrigger;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class SimpleFrontEndTriggerLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(SimpleFrontEndTriggerLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + SimpleFrontEndTriggerLoader.class.getName());
        Loader.main(SimpleFrontEndTriggerLoader.class, args, true);
    }

    public SimpleFrontEndTriggerLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


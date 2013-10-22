package edu.brown.benchmark.nostreamtrigger;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class NoStreamTriggerLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(NoStreamTriggerLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + NoStreamTriggerLoader.class.getName());
        Loader.main(NoStreamTriggerLoader.class, args, true);
    }

    public NoStreamTriggerLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


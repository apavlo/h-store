package edu.brown.benchmark.nostreamtrigger1;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class NoStreamTrigger1Loader extends Loader {

    private static final Logger LOG = Logger.getLogger(NoStreamTrigger1Loader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + NoStreamTrigger1Loader.class.getName());
        Loader.main(NoStreamTrigger1Loader.class, args, true);
    }

    public NoStreamTrigger1Loader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


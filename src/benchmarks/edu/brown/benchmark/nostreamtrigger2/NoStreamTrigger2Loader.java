package edu.brown.benchmark.nostreamtrigger2;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class NoStreamTrigger2Loader extends Loader {

    private static final Logger LOG = Logger.getLogger(NoStreamTrigger2Loader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + NoStreamTrigger2Loader.class.getName());
        Loader.main(NoStreamTrigger2Loader.class, args, true);
    }

    public NoStreamTrigger2Loader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


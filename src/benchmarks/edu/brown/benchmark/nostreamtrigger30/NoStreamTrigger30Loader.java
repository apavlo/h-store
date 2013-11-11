package edu.brown.benchmark.nostreamtrigger30;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class NoStreamTrigger30Loader extends Loader {

    private static final Logger LOG = Logger.getLogger(NoStreamTrigger30Loader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + NoStreamTrigger30Loader.class.getName());
        Loader.main(NoStreamTrigger30Loader.class, args, true);
    }

    public NoStreamTrigger30Loader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


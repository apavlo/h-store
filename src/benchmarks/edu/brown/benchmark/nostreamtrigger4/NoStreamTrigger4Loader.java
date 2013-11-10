package edu.brown.benchmark.nostreamtrigger4;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class NoStreamTrigger4Loader extends Loader {

    private static final Logger LOG = Logger.getLogger(NoStreamTrigger4Loader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + NoStreamTrigger4Loader.class.getName());
        Loader.main(NoStreamTrigger4Loader.class, args, true);
    }

    public NoStreamTrigger4Loader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


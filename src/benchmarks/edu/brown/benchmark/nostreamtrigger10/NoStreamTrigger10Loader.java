package edu.brown.benchmark.nostreamtrigger10;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class NoStreamTrigger10Loader extends Loader {

    private static final Logger LOG = Logger.getLogger(NoStreamTrigger10Loader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + NoStreamTrigger10Loader.class.getName());
        Loader.main(NoStreamTrigger10Loader.class, args, true);
    }

    public NoStreamTrigger10Loader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


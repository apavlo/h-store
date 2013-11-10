package edu.brown.benchmark.nostreamtrigger5;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class NoStreamTrigger5Loader extends Loader {

    private static final Logger LOG = Logger.getLogger(NoStreamTrigger5Loader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + NoStreamTrigger5Loader.class.getName());
        Loader.main(NoStreamTrigger5Loader.class, args, true);
    }

    public NoStreamTrigger5Loader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


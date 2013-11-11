package edu.brown.benchmark.nostreamtrigger20;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class NoStreamTrigger20Loader extends Loader {

    private static final Logger LOG = Logger.getLogger(NoStreamTrigger20Loader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + NoStreamTrigger20Loader.class.getName());
        Loader.main(NoStreamTrigger20Loader.class, args, true);
    }

    public NoStreamTrigger20Loader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


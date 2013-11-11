package edu.brown.benchmark.nostreamtrigger40;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class NoStreamTrigger40Loader extends Loader {

    private static final Logger LOG = Logger.getLogger(NoStreamTrigger40Loader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + NoStreamTrigger40Loader.class.getName());
        Loader.main(NoStreamTrigger40Loader.class, args, true);
    }

    public NoStreamTrigger40Loader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


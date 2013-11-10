package edu.brown.benchmark.nostreamtrigger3;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class NoStreamTrigger3Loader extends Loader {

    private static final Logger LOG = Logger.getLogger(NoStreamTrigger3Loader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + NoStreamTrigger3Loader.class.getName());
        Loader.main(NoStreamTrigger3Loader.class, args, true);
    }

    public NoStreamTrigger3Loader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


package edu.brown.benchmark.simplestreamtrigger;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class SimpleStreamTriggerLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(SimpleStreamTriggerLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + SimpleStreamTriggerLoader.class.getName());
        Loader.main(SimpleStreamTriggerLoader.class, args, true);
    }

    public SimpleStreamTriggerLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


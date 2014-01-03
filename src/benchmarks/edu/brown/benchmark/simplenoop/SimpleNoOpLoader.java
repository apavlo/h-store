package edu.brown.benchmark.simplenoop;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class SimpleNoOpLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(SimpleNoOpLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + SimpleNoOpLoader.class.getName());
        Loader.main(SimpleNoOpLoader.class, args, true);
    }

    public SimpleNoOpLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


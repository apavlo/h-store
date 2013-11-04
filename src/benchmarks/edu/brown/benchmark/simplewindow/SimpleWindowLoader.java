package edu.brown.benchmark.simplewindow;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class SimpleWindowLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(SimpleWindowLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + SimpleWindowLoader.class.getName());
        Loader.main(SimpleWindowLoader.class, args, true);
    }

    public SimpleWindowLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


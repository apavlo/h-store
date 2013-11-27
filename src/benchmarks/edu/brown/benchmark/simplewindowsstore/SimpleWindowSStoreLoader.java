package edu.brown.benchmark.simplewindowsstore;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class SimpleWindowSStoreLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(SimpleWindowSStoreLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + SimpleWindowSStoreLoader.class.getName());
        Loader.main(SimpleWindowSStoreLoader.class, args, true);
    }

    public SimpleWindowSStoreLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


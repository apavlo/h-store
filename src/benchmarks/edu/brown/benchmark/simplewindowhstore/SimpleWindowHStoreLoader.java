package edu.brown.benchmark.simplewindowhstore;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class SimpleWindowHStoreLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(SimpleWindowHStoreLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + SimpleWindowHStoreLoader.class.getName());
        Loader.main(SimpleWindowHStoreLoader.class, args, true);
    }

    public SimpleWindowHStoreLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


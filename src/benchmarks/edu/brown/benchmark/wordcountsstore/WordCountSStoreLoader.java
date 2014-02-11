package edu.brown.benchmark.wordcountsstore;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class WordCountSStoreLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(WordCountSStoreLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + WordCountSStoreLoader.class.getName());
        Loader.main(WordCountSStoreLoader.class, args, true);
    }

    public WordCountSStoreLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


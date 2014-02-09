package edu.brown.benchmark.wordcounthstore;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class WordCountHStoreLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(WordCountHStoreLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + WordCountHStoreLoader.class.getName());
        Loader.main(WordCountHStoreLoader.class, args, true);
    }

    public WordCountHStoreLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


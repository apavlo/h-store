package edu.brown.benchmark.wordcountsstoregetresults;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class WordCountSStoreGetResultsLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(WordCountSStoreGetResultsLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + WordCountSStoreGetResultsLoader.class.getName());
        Loader.main(WordCountSStoreGetResultsLoader.class, args, true);
    }

    public WordCountSStoreGetResultsLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


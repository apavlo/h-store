package edu.brown.benchmark.wordcountsstorewithbatch;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class WordCountSStoreWithBatchLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(WordCountSStoreWithBatchLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + WordCountSStoreWithBatchLoader.class.getName());
        Loader.main(WordCountSStoreWithBatchLoader.class, args, true);
    }

    public WordCountSStoreWithBatchLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


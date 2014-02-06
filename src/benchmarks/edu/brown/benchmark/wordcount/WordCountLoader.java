package edu.brown.benchmark.wordcount;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class WordCountLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(WordCountLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + WordCountLoader.class.getName());
        Loader.main(WordCountLoader.class, args, true);
    }

    public WordCountLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


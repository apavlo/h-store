package edu.brown.benchmark.simplestatement;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class SimpleStatementLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(SimpleStatementLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + SimpleStatementLoader.class.getName());
        Loader.main(SimpleStatementLoader.class, args, true);
    }

    public SimpleStatementLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


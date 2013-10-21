package edu.brown.benchmark.streamtrigger;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class StreamTriggerLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(StreamTriggerLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + StreamTriggerLoader.class.getName());
        Loader.main(StreamTriggerLoader.class, args, true);
    }

    public StreamTriggerLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
    }
}


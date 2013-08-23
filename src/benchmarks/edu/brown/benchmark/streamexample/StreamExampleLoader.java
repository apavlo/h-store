package edu.brown.benchmark.streamexample;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class StreamExampleLoader extends Loader {
	
	private static final Logger LOG = Logger.getLogger(StreamExampleLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + StreamExampleLoader.class.getName());
        Loader.main(StreamExampleLoader.class, args, true);
    }

    public StreamExampleLoader(String[] args) {
        super(args);
        if (d) LOG.debug("CONSTRUCTOR: " + StreamExampleLoader.class.getName());
    }

    @Override
    public void load() {
        if (d) 
            LOG.debug("Starting StreamExampleLoader");

        try {
            this.getClientHandle().callProcedure("Initialize");
            //this.getClientHandle().callProcedure("Initialize2");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

package edu.brown.benchmark.complexstreamtrigger;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class ComplexStreamTriggerLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(ComplexStreamTriggerLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + ComplexStreamTriggerLoader.class.getName());
        Loader.main(ComplexStreamTriggerLoader.class, args, true);
    }

    public ComplexStreamTriggerLoader(String[] args) {
        super(args);
    }

    @Override
    public void load() {
        try {
            this.getClientHandle().callProcedure("Initialize",
                                                 1000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}


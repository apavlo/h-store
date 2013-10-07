package edu.brown.benchmark.anotherstream;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class AnotherStreamLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(AnotherStreamLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + AnotherStreamLoader.class.getName());
        Loader.main(AnotherStreamLoader.class, args, true);
    }

    public AnotherStreamLoader(String[] args) {
        super(args);
        if (d) LOG.debug("CONSTRUCTOR: " + AnotherStreamLoader.class.getName());
    }

    @Override
    public void load() {
        int numContestants = VoterUtil.getScaledNumContestants(this.getScaleFactor());
        if (d) 
            LOG.debug("Starting VoterLoader [numContestants=" + numContestants + "]");

        try {
            this.getClientHandle().callProcedure("Initialize",
                                                 numContestants,
                                                 VoterConstants.CONTESTANT_NAMES_CSV);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}


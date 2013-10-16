package edu.brown.benchmark.anothervoterstream;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class AnotherVoterStreamLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(AnotherVoterStreamLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + AnotherVoterStreamLoader.class.getName());
        Loader.main(AnotherVoterStreamLoader.class, args, true);
    }

    public AnotherVoterStreamLoader(String[] args) {
        super(args);
        if (d) LOG.debug("CONSTRUCTOR: " + AnotherVoterStreamLoader.class.getName());
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


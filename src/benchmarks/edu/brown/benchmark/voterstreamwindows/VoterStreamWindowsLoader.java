package edu.brown.benchmark.voterstreamwindows;

import org.apache.log4j.Logger;

import edu.brown.api.Loader;

public class VoterStreamWindowsLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(VoterStreamWindowsLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
        if (d) LOG.debug("MAIN: " + VoterStreamWindowsLoader.class.getName());
        Loader.main(VoterStreamWindowsLoader.class, args, true);
    }

    public VoterStreamWindowsLoader(String[] args) {
        super(args);
        if (d) LOG.debug("CONSTRUCTOR: " + VoterStreamWindowsLoader.class.getName());
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


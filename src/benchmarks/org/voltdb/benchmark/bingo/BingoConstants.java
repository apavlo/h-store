package org.voltdb.benchmark.bingo;

public abstract class BingoConstants {

    static int maxTournaments = 1000000;
    static int maxRounds = 100;
    static int boardsPerTournament = 10;

    /** The probability that we will invoke GetAveragePot (0% - 100%) */
    public static final int GETAVGPOT_MIN = 29998;
    public static final int GETAVGPOT_MAX = 30000;
}

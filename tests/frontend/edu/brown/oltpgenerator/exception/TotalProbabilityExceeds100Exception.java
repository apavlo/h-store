package edu.brown.oltpgenerator.exception;

public class TotalProbabilityExceeds100Exception extends Exception
{
    private static final long serialVersionUID = 1L;

    public TotalProbabilityExceeds100Exception(Integer nOldVal, int nNewVal)
    {
        super("Changing probability from " + nOldVal + " to " + nNewVal + " will cause total probability excceds 100");
    }
}

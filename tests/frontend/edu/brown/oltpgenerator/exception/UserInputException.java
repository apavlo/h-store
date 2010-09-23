package edu.brown.oltpgenerator.exception;

public class UserInputException extends Exception
{
    private static final long serialVersionUID = 1L;

    public UserInputException(String errMsg)
    {
        super(errMsg);
    }
}

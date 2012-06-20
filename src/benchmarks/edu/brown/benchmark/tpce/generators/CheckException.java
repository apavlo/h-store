package edu.brown.benchmark.tpce.generators;

class CheckException extends Exception{
    
    private static final long serialVersionUID = 1L;
    
    private Throwable myThrow;
    
    public CheckException(){
        super();
    }
    
    public CheckException(String name, Throwable myThrow){
        super(name, myThrow);
        this.myThrow = myThrow;
    }
    
    public int errorType(){
        return exceptionType.ERR_TYPE_CHECK;
    }
    
    public String errorText(){
        return myThrow.toString();
    }
}

class exceptionType{
    public static final int ERR_TYPE_LOGIC          = -1;      //logic error in program; internal error
    public static final int ERR_SUCCESS             = 0;       //success (a non-error error)
    public static final int ERR_TYPE_OS             = 11;      //operating system error
    public static final int ERR_TYPE_MEMORY         = 12;      //memory allocation error
    public static final int ERR_TYPE_FIXED_MAP      = 27;      //Error from CFixedMap
    public static final int ERR_TYPE_FIXED_ARRAY    = 28;      //Error from CFixedArray
    public static final int ERR_TYPE_CHECK          = 29;      //Check assertion error (from DriverParamSettings)

    public static final String ERR_INS_MEMORY       =  "Insufficient Memory to continue.";
    public static final String ERR_UNKNOWN          =  "Unknown error.";
    public static final int ERR_MSG_BUF_SIZE        = 512;
    public static final int INV_ERROR_CODE          = -1;
}

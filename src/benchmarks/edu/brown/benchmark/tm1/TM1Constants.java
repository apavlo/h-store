package edu.brown.benchmark.tm1;

public abstract class TM1Constants {

    // ----------------------------------------------------------------
    // STORED PROCEDURE EXECUTION FREQUENCIES (0-100)
    // ----------------------------------------------------------------
    public static final int FREQUENCY_DELETE_CALL_FORWARDING = 0; // Multi
    //public static final int FREQUENCY_DELETE_CALL_FORWARDING = 2; // Multi
    public static final int FREQUENCY_GET_ACCESS_DATA = 39; // Single
    public static final int FREQUENCY_GET_NEW_DESTINATION = 15; // Single
    public static final int FREQUENCY_GET_SUBSCRIBER_DATA = 39; // Single
    public static final int FREQUENCY_INSERT_CALL_FORWARDING = 0; // Multi
    //public static final int FREQUENCY_INSERT_CALL_FORWARDING = 2; // Multi
    public static final int FREQUENCY_UPDATE_LOCATION = 0; // Multi
    //public static final int FREQUENCY_UPDATE_LOCATION = 14; // Multi
    public static final int FREQUENCY_UPDATE_SUBSCRIBER_DATA = 7; // Single

    // ----------------------------------------------------------------
    // TABLE NAMES
    // ----------------------------------------------------------------
    public static final String TABLENAME_SUBSCRIBER = "SUBSCRIBER";
    public static final String TABLENAME_ACCESS_INFO = "ACCESS_INFO";
    public static final String TABLENAME_SPECIAL_FACILITY = "SPECIAL_FACILITY";
    public static final String TABLENAME_CALL_FORWARDING = "CALL_FORWARDING";

    public static final int SUBSCRIBER_SIZE = 100000;
    
    public static final int BATCH_SIZE = 5000;

    public static final String TABLENAMES[] = { TABLENAME_SUBSCRIBER,
                                                TABLENAME_ACCESS_INFO,
                                                TABLENAME_SPECIAL_FACILITY,
                                                TABLENAME_CALL_FORWARDING };
}

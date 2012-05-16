package com.mytest.benchmark.abc;

;

public abstract class ABCConstants {

	public enum ExecutionType {
		SAME_PARTITION, SAME_SITE, SAME_HOST, REMOTE_HOST, RANDOM;
	}

	// ----------------------------------------------------------------
	// TABLE INFORMATION
	// ----------------------------------------------------------------

	public static final String TABLENAME_TABLEA = "TABLEA";
	public static final long TABLESIZE_TABLEA = 1000000l;
	public static final long BATCHSIZE_TABLEA = 10000l;

	public static final String TABLENAME_TABLEB = "TABLEB";
	public static final double TABLESIZE_TABLEB_MULTIPLIER = 10.0d;
	public static final long TABLESIZE_TABLEB = Math
			.round(ABCConstants.TABLESIZE_TABLEA * TABLESIZE_TABLEB_MULTIPLIER);
	public static final long BATCHSIZE_TABLEB = 10000l;

	public static final String[] TABLENAMES = { TABLENAME_TABLEA,
			TABLENAME_TABLEB, };

	// ----------------------------------------------------------------
	// STORED PROCEDURE INFORMATION
	// ----------------------------------------------------------------

	public static final int FREQUENCY_GET = 100;
	public static final int FREQUENCY_SELECT = 50;
	public static final int FREQUENCY_GET_REMOTE = 0;
	public static final int FREQUENCY_SET_REMOTE = 0;

	// The number of TABLEB records to return per GetLocal/GetRemote invocation
	public static final int GET_TABLEB_LIMIT = 10;

}

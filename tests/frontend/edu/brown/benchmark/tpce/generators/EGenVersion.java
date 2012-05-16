package edu.brown.benchmark.tpce.generators;

public class EGenVersion {

	public static int    iEGenMajorVersion   = 1;    // major revision number
	public static int    iEGenMinorVersion   = 3;    // minor revision number
	public static int    iEGenRevisionNumber = 0;    // third-tier revision number
	public static int    iEGenBetaLevel      = 0;    // beta version (for maintenance only) 
	
	// Retrieve major, minor, revision, and beta level numbers for EGen.
	//
	public static void GetEGenVersion(int  iMajorVersion, int  iMinorVersion, int  iRevisionNumber, int  iBetaLevel)
	{
	    iMajorVersion = iEGenMajorVersion;
	    iMinorVersion = iEGenMinorVersion;
	    iRevisionNumber = iEGenRevisionNumber;
	    iBetaLevel = iEGenBetaLevel;
	}

	// Return versioning information formated as a string
	//
	public static void GetEGenVersionString(char[] szOutput, int iOutputBufferLen)
	{
	    int iLen;

	    String egenVersion = new String("EGen v" + iEGenMajorVersion + "." + iEGenMinorVersion + "." +iEGenRevisionNumber);
	    iLen = egenVersion.length();

	    if (iEGenBetaLevel != 0)
	    {
	    	egenVersion.concat("beta" + iEGenBetaLevel);
	    	iLen = egenVersion.length();
	    }
	    
	    if (iLen > iOutputBufferLen) {
	        if (iOutputBufferLen > 0) {
	            szOutput[0] = '\n';
	        }
	    } else {
	    	System.arraycopy(egenVersion.toCharArray(), 0, szOutput, 0, iLen);
	    }
	}
/*
	// Output EGen versioning information on stdout
	//
	public static void PrintEGenVersion()
	{
	    char    szVersion[33];

	    GetEGenVersionString(szVersion, static_cast<int>(sizeof(szVersion)-1));

	    printf("%s\n", szVersion);
	}

	// Return the date/time when the EGen versioning information was last updated.
	//
	public static void GetEGenVersionUpdateTimestamp(char* szOutput, int iOutputBufferLen)
	{
	    strncpy(szOutput, __DATE__" "__TIME__, iOutputBufferLen);
	}

	}

	}   // namespace TPC-E*/

}

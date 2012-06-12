package edu.brown.benchmark.tpce.generators;

public class EGenVersion {

    public static int    iEGenMajorVersion   = 1;    // major revision number
    public static int    iEGenMinorVersion   = 3;    // minor revision number
    public static int    iEGenRevisionNumber = 0;    // third-tier revision number
    public static int    iEGenBetaLevel      = 0;    // beta version (for maintenance only) 
    
    public static void getEGenVersion(int  majorVersion, int  minorVersion, int  revisionNumber, int  betaLevel){
        majorVersion = iEGenMajorVersion;
        minorVersion = iEGenMinorVersion;
        revisionNumber = iEGenRevisionNumber;
        betaLevel = iEGenBetaLevel;
    }
    
    public static String getEGenVersionString(int iOutputBufferLen){
        int iLen;

        String egenVersion = new String("EGen v" + iEGenMajorVersion + "." + iEGenMinorVersion + "." +iEGenRevisionNumber);
        iLen = egenVersion.length();

        if (iEGenBetaLevel != 0){
        	egenVersion = egenVersion.concat("beta" + iEGenBetaLevel);
            iLen = egenVersion.length();
        }
        
        if (iLen > iOutputBufferLen) {
//            if (iOutputBufferLen > 0) {
                return new String();
//            }
        } else {
            return egenVersion;
        }
    }
}

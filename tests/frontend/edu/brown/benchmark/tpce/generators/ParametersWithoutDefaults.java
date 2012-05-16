package edu.brown.benchmark.tpce.generators;

public abstract class ParametersWithoutDefaults {
	
	public ParametersWithoutDefaults(){}
	public abstract void CheckValid();
	public abstract void CheckCompliant();
	
	public abstract boolean IsValid();
    public abstract boolean IsCompliant();
/*    
    private ArrayList<Object>  TBrokerVolumeSettings;
    private ArrayList<Object>  TCustomerPositionSettings;
    private ArrayList<Object>  TMarketWatchSettings;
    private ArrayList<Object>  TSecurityDetailSettings;
    private ArrayList<Object>  TTradeLookupSettings;
    private ArrayList<Object>  TTradeOrderSettings;
    private ArrayList<Object>  TTradeUpdateSettings;
    private ArrayList<Object>  TTxnMixGeneratorSettings;
    private ArrayList<Object>  TLoaderSettings;
    private ArrayList<Object>  TDriverGlobalSettings;
    private ArrayList<Object>  TDriverCESettings;
    private ArrayList<Object>  TDriverCEPartitionSettings;
    private ArrayList<Object>  TDriverMEESettings;
    private ArrayList<Object>  TDriverDMSettings;
    
    private ArrayList<Object>  TBrokerVolumeSettingsState;
    private ArrayList<Object>  TMarketWatchSettingsState;
    private ArrayList<Object>  TSecurityDetailSettingsState;
    private ArrayList<Object>  TTradeLookupSettingsState;
    private ArrayList<Object>  TTradeOrderSettingsState;
    private ArrayList<Object>  TTradeUpdateSettingsState;
    private ArrayList<Object>  TTxnMixGeneratorSettingsState;
    private ArrayList<Object>  TLoaderSettingsState;
    private ArrayList<Object>  TDriverCEPartitionSettingsState;
    private ArrayList<Object>  TDriverGlobalSettingsState;
    **/
    public void driverParamCheckEqual(String name, int lhs, int rhs) throws checkException{
		if (lhs != rhs){
			String strm = new String("(" + lhs + ") !=" + "(" + rhs + ")");
			Throwable myThrow = new Throwable(strm);
			throw new checkException(name, myThrow);
		}
	}
    
    public void driverParamCheckGE(String name, int lhs, int rhs) throws checkException{
		if (lhs < rhs){
			String strm = new String("(" + lhs + ") <" + "(" + rhs + ")");
			Throwable myThrow = new Throwable(strm);
			throw new checkException(name, myThrow);
		}
	}
    
    public void driverParamCheckLE(String name, int lhs, int rhs) throws checkException{
		if (lhs > rhs){
			String strm = new String("(" + lhs + ") <" + "(" + rhs + ")");
			Throwable myThrow = new Throwable(strm);
			throw new checkException(name, myThrow);
		}
	}
    
    public void driverParamCheckBetween(String name, int lhs, int minval, int maxval) throws checkException{
    	driverParamCheckLE(name, lhs, minval);
    	driverParamCheckLE(name, lhs, maxval);
	}
    
    public void driverParamCheckDefault(Object settings1, Object settings2, String name) throws checkException{
    	if (false == settings1.equals(settings2)){
    		String strm = new String("(" + name + ") !=" + "(" + name + ")");
			Throwable myThrow = new Throwable(strm);
			throw new checkException(name, myThrow);
    	}
    }
}

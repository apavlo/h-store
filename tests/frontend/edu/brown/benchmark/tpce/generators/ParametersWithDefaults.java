package edu.brown.benchmark.tpce.generators;

public abstract class ParametersWithDefaults {

	
    public ParametersWithDefaults() {}
    

    public void Initialize()
    {
        InitializeDefaults();
        SetToDefaults();
    }

    public void SetToDefaults()
    { 
        CheckDefaults();
    }

    public abstract void InitializeDefaults();
    public abstract void CheckDefaults();
    public abstract boolean CheckValid();
    public abstract boolean CheckCompliant();
    
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
    	driverParamCheckGE(name, lhs, minval);
    	driverParamCheckLE(name, lhs, maxval);
	}
    
    public void driverParamCheckDefault(Object dft_settings, Object cur_settings, String name) throws checkException{
    	if (false == dft_settings.equals(cur_settings)){
    		String strm = new String("(" + "dft_" + name + ") !=" + "(" + "cur_" + name + ")");
			Throwable myThrow = new Throwable(strm);
			throw new checkException(name, myThrow);
    	}
    }

}

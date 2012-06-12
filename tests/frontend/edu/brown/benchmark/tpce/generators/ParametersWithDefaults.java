package edu.brown.benchmark.tpce.generators;

public abstract class ParametersWithDefaults {

    public ParametersWithDefaults() {}
    public void initialize(){
        initializeDefaults();
        setToDefaults();
    }

    public void setToDefaults(){ 
        checkDefaults();
    }

    public abstract void initializeDefaults();
    public abstract void checkDefaults();
    public abstract boolean checkValid();
    public abstract boolean checkCompliant();
    
    public void driverParamCheckEqual(String name, int lhs, int rhs) throws CheckException{
        if (lhs != rhs){
            String strm = new String("(" + lhs + ") !=" + "(" + rhs + ")");
            Throwable myThrow = new Throwable(strm);
            throw new CheckException(name, myThrow);
        }
    }
    
    public void driverParamCheckGE(String name, int lhs, int rhs) throws CheckException{
        if (lhs < rhs){
            String strm = new String("(" + lhs + ") <" + "(" + rhs + ")");
            Throwable myThrow = new Throwable(strm);
            throw new CheckException(name, myThrow);
        }
    }
    
    public void driverParamCheckLE(String name, int lhs, int rhs) throws CheckException{
        if (lhs > rhs){
            String strm = new String("(" + lhs + ") <" + "(" + rhs + ")");
            Throwable myThrow = new Throwable(strm);
            throw new CheckException(name, myThrow);
        }
    }
    
    public void driverParamCheckBetween(String name, int lhs, int minval, int maxval) throws CheckException{
        driverParamCheckGE(name, lhs, minval);
        driverParamCheckLE(name, lhs, maxval);
    }
    
    public void driverParamCheckDefault(Object dft_settings, Object cur_settings, String name) throws CheckException{
        if (false == dft_settings.equals(cur_settings)){
            String strm = new String("(" + "dft_" + name + ") !=" + "(" + "cur_" + name + ")");
            Throwable myThrow = new Throwable(strm);
            throw new CheckException(name, myThrow);
        }
    }

}

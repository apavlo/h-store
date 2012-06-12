package edu.brown.benchmark.tpce.generators;

public abstract class ParametersWithoutDefaults {
    
    public ParametersWithoutDefaults(){}
    public abstract void checkValid();
    public abstract void checkCompliant();
    
    public abstract boolean isValid();
    public abstract boolean isCompliant();

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
        driverParamCheckLE(name, lhs, minval);
        driverParamCheckLE(name, lhs, maxval);
    }
    
    public void driverParamCheckDefault(Object settings1, Object settings2, String name) throws CheckException{
        if (false == settings1.equals(settings2)){
            String strm = new String("(" + name + ") !=" + "(" + name + ")");
            Throwable myThrow = new Throwable(strm);
            throw new CheckException(name, myThrow);
        }
    }
}

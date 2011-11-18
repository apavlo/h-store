package edu.brown.logging;

import org.apache.log4j.spi.LocationInfo;

public class FastLocationInfo extends LocationInfo {
    private static final long serialVersionUID = -7253756673938361338L;
    
    private final String lineNumber;
    private final String fileName;
    private final String className;
    private final String methodName;
    
    public FastLocationInfo(int lineNumber, String fileName, String className, String methodName) {
        super(null, null);
        this.lineNumber = Integer.toString(lineNumber);
        this.fileName = fileName;
        this.className = className;
        this.methodName = methodName;
    }
    
    @Override
    public String getLineNumber() {
        return (this.lineNumber);
    }
    @Override
    public String getFileName() {
        return (this.fileName);
    }
    @Override
    public String getClassName() {
        return (this.className);
    }
    @Override
    public String getMethodName() {
        return (this.methodName);
    }
    
    @Override
    public String toString() {
        return String.format("%s:%s -> %s.%s()",
                             this.fileName, this.lineNumber, this.className, this.methodName); 
    }
}

package edu.brown.catalog.conflicts;

public class ConflictParameterPair {

    public static final int NULL_OFFSET = -1;
    
    private final int procParam0;
    private final int procParamOffset0;
    
    private final int procParam1;
    private final int procParamOffset1;
    
    protected ConflictParameterPair(int param0[], int param1[]) {
        this(param0[0], param0[1], param1[0], param1[1]);
    }
    
    public ConflictParameterPair(int procParam0, int procParamOffset0, int procParam1, int procParamOffset1) {
        this.procParam0 = procParam0;
        this.procParamOffset0 = procParamOffset0;
        this.procParam1 = procParamOffset1;
        this.procParamOffset1 = procParamOffset1;
    }
    
    public int getProcParam0() {
        return (this.procParam0);
    }
    public boolean hasOffset0() {
        return this.procParamOffset0 != NULL_OFFSET;
    }
    
    public int getProcParam1() {
        return (this.procParam1);
    }
    public boolean hasOffset1() {
        return this.procParamOffset1 != NULL_OFFSET;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ConflictParameterPair) {
            ConflictParameterPair other = (ConflictParameterPair)obj;
            return (this.procParam0 == other.procParam0 &&
                    this.procParamOffset0 == other.procParamOffset0 &&
                    this.procParam1 == other.procParam1 &&
                    this.procParamOffset0 == other.procParamOffset0);
        }
        return (false);
    }
    
}

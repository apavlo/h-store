package edu.brown.designer.partitioners.plan;

import org.voltdb.catalog.ProcParameter;
import org.voltdb.types.PartitionMethodType;

public class ProcedureEntry extends PartitionEntry<ProcParameter> {

    // Procedure Information
    public Boolean single_partition;

    public ProcedureEntry() {
        // For serialziation
    }

    public ProcedureEntry(PartitionMethodType method) {
        this(method, null, null);
    }

    public ProcedureEntry(PartitionMethodType method, ProcParameter catalog_param, Boolean single_partition) {
        super(method, catalog_param);
        this.single_partition = single_partition;
    }

    /**
     * Is the procedure this entry guaranteed to be single-partition?
     * 
     * @return
     */
    public Boolean isSinglePartition() {
        return single_partition;
    }

    public void setSinglePartition(boolean singlePartition) {
        this.single_partition = singlePartition;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ProcedureEntry)) {
            return (false);
        }
        ProcedureEntry other = (ProcedureEntry) obj;

        // SinglePartition
        if (this.single_partition == null) {
            if (other.single_partition != null)
                return (false);
        } else if (!this.single_partition.equals(other.single_partition)) {
            return (false);
        }

        return (super.equals(other));
    }

}

/**
 * 
 */
package edu.brown.utils;

import java.util.Collection;
import java.util.HashSet;

/**
 * Container class that represents a list of partitionIds
 * For now it's just a HashSet
 * @author pavlo
 */
public class PartitionSet extends HashSet<Integer> {
    private static final long serialVersionUID = 1;
    
    public PartitionSet() {
        super();
    }
    
    public PartitionSet(Collection<Integer> partitions) {
        super(partitions);
    }
    
    public PartitionSet(Integer...partitions) {
        super();
        for (Integer p : partitions)
            this.add(p);
    }

}

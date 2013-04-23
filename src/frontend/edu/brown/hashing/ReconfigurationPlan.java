/**
 * 
 */
package edu.brown.hashing;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.voltdb.VoltType;

import edu.brown.hashing.PlannedPartitions.PartitionPhase;
import edu.brown.hashing.PlannedPartitions.PartitionRange;
import edu.brown.hashing.PlannedPartitions.PartitionedTable;

/**
 * The delta between two partition plans
 * @author aelmore
 *
 */
public class ReconfigurationPlan {
    Map<String,ReconfigurationTable<? extends Comparable<?>>> tables_map;
    
    //Helper map of partition ID and outgoing/incoming ranges for this reconfiguration
    protected Map<Integer, List<ReconfigurationRange<? extends Comparable<?>>>> outgoing_ranges;
    protected Map<Integer, List<ReconfigurationRange<? extends Comparable<?>>>> incoming_ranges;
    
    /**
     * @throws Exception 
     * 
     */
    public ReconfigurationPlan(PartitionPhase old_phase,PartitionPhase new_phase) throws Exception {
        outgoing_ranges = new HashMap<>();
        incoming_ranges = new HashMap<>();
        assert old_phase.tables_map.keySet().equals(new_phase.tables_map.keySet()) : "Partition plans have different tables";
        tables_map = new HashMap<String, ReconfigurationPlan.ReconfigurationTable<? extends Comparable<?>>>();
        for(String table_name : old_phase.tables_map.keySet()){
            tables_map.put(table_name, new ReconfigurationTable(old_phase.getTable(table_name), new_phase.getTable(table_name)));
        }
        registerReconfigurationRanges();
    }
    
    protected void registerReconfigurationRanges(){
        for(String table_name : tables_map.keySet()){
            for(ReconfigurationRange<?> range : tables_map.get(table_name).reconfigurations){
                if(outgoing_ranges.containsKey(range.old_partition)==false){
                    outgoing_ranges.put(range.old_partition, new ArrayList<ReconfigurationRange<? extends Comparable<?>>>());
                }
                if(incoming_ranges.containsKey(range.new_partition)==false){
                    incoming_ranges.put(range.new_partition, new ArrayList<ReconfigurationRange<? extends Comparable<?>>>());
                }
                outgoing_ranges.get(range.old_partition).add(range);
                incoming_ranges.get(range.new_partition).add(range);
            }
        }
    }

    public static class ReconfigurationTable<T extends Comparable<T>> {
        List<ReconfigurationRange<T>> reconfigurations;
        String table_name;
        public ReconfigurationTable(PartitionedTable<T> old_table, PartitionedTable<T> new_table) throws Exception {
          table_name = old_table.table_name;
          reconfigurations = new ArrayList<ReconfigurationRange<T>>();
          Iterator<PartitionRange<T>> old_ranges = old_table.partitions.iterator();
          Iterator<PartitionRange<T>> new_ranges = new_table.partitions.iterator();

          PartitionRange<T> new_range = new_ranges.next();
          T max_old_accounted_for = null;
          PartitionRange<T> old_range = null;
          // Iterate through the old partition ranges.
          // Only move to the next old rang
          while (old_ranges.hasNext() || (max_old_accounted_for != null && max_old_accounted_for.compareTo(old_range.max_exclusive)!=0) ) {
            // only move to the next element if first time, or all of the previous
            // range has been accounted for
            if (old_range == null || old_range.max_exclusive.compareTo(max_old_accounted_for) <= 0) {
              old_range = old_ranges.next();
            }

            if (max_old_accounted_for == null) {
              // We have not accounted for any range yet
              max_old_accounted_for = old_range.min_inclusive;
            }
            if (old_range.compareTo(new_range) == 0) {
              if (old_range.partition == new_range.partition) {
                // No change do nothing
              } else {
                // Same range new partition
                reconfigurations.add(new ReconfigurationRange<T>(table_name, old_range.vt, old_range.min_inclusive, old_range.max_exclusive,
                    old_range.partition, new_range.partition));
              }
              max_old_accounted_for = old_range.max_exclusive;
              new_range = new_ranges.next();
            } else {
              if (old_range.max_exclusive.compareTo(new_range.max_exclusive) <= 0) {
                // The old range is a subset of the new range
                if (old_range.partition == new_range.partition) {
                  // Same partitions no reconfiguration needed here
                  max_old_accounted_for = old_range.max_exclusive;
                } else {
                  // Need to move the old range to new range
                  reconfigurations.add(new ReconfigurationRange<T>(table_name, old_range.vt, max_old_accounted_for, old_range.max_exclusive,
                      old_range.partition, new_range.partition));
                  max_old_accounted_for = old_range.max_exclusive;
                  
                  //Have we satisfied all of the new range and is there another new range to process
                  if (max_old_accounted_for.compareTo(new_range.max_exclusive)==0 && new_ranges.hasNext()){
                    new_range = new_ranges.next();
                  }
                }
              } else {
                // The old range is larger than this new range
                // keep getting new ranges until old range has been satisfied
                while (old_range.max_exclusive.compareTo(new_range.max_exclusive) > 0) {
                  if (old_range.partition == new_range.partition) {
                    // No need to move this range
                    max_old_accounted_for = new_range.max_exclusive;
                  } else {
                    // move
                    reconfigurations.add(new ReconfigurationRange<T>(table_name, old_range.vt, max_old_accounted_for, new_range.max_exclusive,
                        old_range.partition, new_range.partition));
                    max_old_accounted_for = new_range.max_exclusive;
                  }
                  if (new_ranges.hasNext() == false) {
                    throw new RuntimeException("Not all ranges accounted for");
                  }
                  new_range = new_ranges.next();
                }
              }

            }
          }
        }
      }
      
      /**
       * A partition range that holds old and new partition IDs
       * 
       * @author aelmore
       * 
       * @param <T>
       */
      public static class ReconfigurationRange<T extends Comparable<T>> extends PartitionRange<T> {
        public int old_partition;
        public int new_partition;
        public String table_name;

        public ReconfigurationRange(String table_name, VoltType vt, T min_inclusive, T max_exclusive, int old_partition, int new_partition) throws ParseException {
          super(vt, min_inclusive, max_exclusive);
          this.old_partition = old_partition;
          this.new_partition = new_partition;
          this.table_name = table_name;
        }
        
        @Override
        public String toString(){
          return String.format("ReconfigRange [%s,%s) id:%s->%s ",min_inclusive,max_exclusive,old_partition,new_partition);
        }
      }

    public Map<Integer, List<ReconfigurationRange<? extends Comparable<?>>>> getOutgoing_ranges() {
        return outgoing_ranges;
    }

    public Map<Integer, List<ReconfigurationRange<? extends Comparable<?>>>> getIncoming_ranges() {
        return incoming_ranges;
    }
}

package edu.brown.hstore.internal;

import java.util.Collection;

import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;

import edu.brown.utils.EventObservable;

/**
 * This is just a placeholder for now to indicate that we completed
 * utility work in the PartitionExecutor. Eventually we will
 * want to put actual work inside of these things
 * @author pavlo
 */
public class UtilityWorkMessage extends InternalMessage {

    /**
     * Update Memory Stats 
     *
     */
    public static class UpdateMemoryMessage extends UtilityWorkMessage {

    }
    
   /**
    * Table Stats Request
    */
   public static class TableStatsRequestMessage extends UtilityWorkMessage {
       
       private final int[] locators;
       private final EventObservable<VoltTable> observable = new EventObservable<VoltTable>();
       
       public TableStatsRequestMessage(Collection<Table> catalog_tbls) {
           this(catalog_tbls.toArray(new Table[0]));
       }
       
       public TableStatsRequestMessage(Table...catalog_tbls) {
           this.locators = new int[catalog_tbls.length];
           for (int i = 0; i < this.locators.length; i++) {
               this.locators[i] = catalog_tbls[i].getRelativeIndex();
           }
       }
       
       public EventObservable<VoltTable> getObservable() {
           return (this.observable);
       }
       
       public int[] getLocators() {
           return (this.locators);
       }

   }
    
}

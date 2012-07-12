package edu.brown.hstore.internal;

import java.util.Collection;

import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;

import edu.brown.utils.EventObservable;

/**
 *
 */
public class TableStatsRequestMessage extends InternalMessage {
    
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

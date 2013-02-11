package edu.brown.hstore.stats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.voltdb.StatsSource;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

import edu.brown.hstore.HStoreObjectPools;
import edu.brown.pools.TypedObjectPool;
import edu.brown.pools.TypedPoolableObjectFactory;

public class PoolCounterStats extends StatsSource {
    
    private final HStoreObjectPools objectPools;
    private final Map<String, TypedObjectPool<?>> globalPools;
    private final Map<String, TypedObjectPool<?>[]> partitionPools;
    private final List<Object> allPoolNames = new ArrayList<Object>();

    public PoolCounterStats(HStoreObjectPools objectPools) {
        super(SysProcSelector.POOL.name(), false);
        this.objectPools = objectPools;
        this.globalPools = this.objectPools.getGlobalPools();
        this.partitionPools = this.objectPools.getPartitionedPools();
        
        this.allPoolNames.addAll(this.globalPools.keySet());
        this.allPoolNames.addAll(this.partitionPools.keySet());
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return (this.allPoolNames.iterator());
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new VoltTable.ColumnInfo("POOL_NAME", VoltType.STRING));
        columns.add(new VoltTable.ColumnInfo("IS_GLOBAL", VoltType.BOOLEAN));
        columns.add(new VoltTable.ColumnInfo("ACTIVE", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("IDLE", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("CREATED", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("DESTROYED", VoltType.INTEGER));
        columns.add(new VoltTable.ColumnInfo("PASSIVATED", VoltType.INTEGER));
    }

    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
        String poolName = (String)rowKey;
        boolean isGlobal;
        int total_active = 0;
        int total_idle = 0;
        int total_created = 0;
        int total_passivated = 0;
        int total_destroyed = 0;
        
        if (this.globalPools.containsKey(poolName)) {
            isGlobal = true;
            TypedObjectPool<?> pool = this.globalPools.get(poolName);
            TypedPoolableObjectFactory<?> factory = (TypedPoolableObjectFactory<?>)pool.getFactory();
            
            total_active = pool.getNumActive();
            total_idle = pool.getNumIdle(); 
            total_created = factory.getCreatedCount();
            total_passivated = factory.getPassivatedCount();
            total_destroyed = factory.getDestroyedCount();
        }
        else if (this.partitionPools.containsKey(poolName)) {
            isGlobal = false;
            TypedObjectPool<?> pools[] = this.partitionPools.get(poolName);
            for (int i = 0; i < pools.length; i++) {
                if (pools[i] == null) continue;
                TypedPoolableObjectFactory<?> factory = (TypedPoolableObjectFactory<?>)pools[i].getFactory();
                
                total_active += pools[i].getNumActive();
                total_idle += pools[i].getNumIdle(); 
                total_created += factory.getCreatedCount();
                total_passivated += factory.getPassivatedCount();
                total_destroyed += factory.getDestroyedCount();
            } // FOR
        }
        else {
            throw new RuntimeException("Unexpected '" + poolName + "'");
        }
        
        rowValues[columnNameToIndex.get("POOL_NAME")] = poolName;
        rowValues[columnNameToIndex.get("IS_GLOBAL")] = isGlobal;
        rowValues[columnNameToIndex.get("ACTIVE")] = total_active;
        rowValues[columnNameToIndex.get("IDLE")] = total_idle;
        rowValues[columnNameToIndex.get("CREATED")] = total_created;
        rowValues[columnNameToIndex.get("DESTROYED")] = total_destroyed;
        rowValues[columnNameToIndex.get("PASSIVATED")] = total_passivated;
        super.updateStatsRow(rowKey, rowValues);
    }
}

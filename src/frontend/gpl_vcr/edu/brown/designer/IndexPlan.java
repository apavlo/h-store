package edu.brown.designer;

import java.util.*;

import org.voltdb.catalog.*;

public class IndexPlan extends HashMap<Table, Set<IndexPlan.Entry>> {
    private static final long serialVersionUID = 1L;

    /**
     * Candidate Index Entry
     */
    public class Entry {
        private final Table catalog_tbl;
        private final List<Column> columns = new ArrayList<Column>();
        private final Set<Procedure> procedures = new HashSet<Procedure>();
        private double weight = 0;
        
        public Entry(Table catalog_tbl) {
            this.catalog_tbl = catalog_tbl;
        }
        
        /**
         * Merge the source index information into our object
         * @param source
         */
        public void merge(Entry source) {
            assert(this.catalog_tbl == source.catalog_tbl);
            this.procedures.addAll(source.procedures);
            this.weight += source.weight;
        }
        
        public Table getTable() {
            return this.catalog_tbl;
        }
        
        public List<Column> getColumns() {
            return this.columns;
        }
        
        public Set<Procedure> getProcedures() {
            return this.procedures;
        }
        
        public double getWeight() {
            return this.weight;
        }
        
        public void setWeight(double weight) {
            this.weight = weight;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Entry) {
                Entry other = (Entry)obj;
                if (this.catalog_tbl != other.catalog_tbl) return (false);
                return (this.columns.equals(other.columns));
            }
            return (false);
        }
        
        @Override
        public String toString() {
            String ret = catalog_tbl.getName() + " (";
            String add = "";
            
            //
            // Super Lame...
            //
            Vector<Column> sorted = new Vector<Column>();
            sorted.setSize(this.catalog_tbl.getColumns().size());
            for (Column catalog_col : this.columns) {
                sorted.set(catalog_col.getIndex(), catalog_col);
            } // FOR
            for (Column catalog_col : sorted) {
                if (catalog_col != null) {
                    ret += add + catalog_col.getName();
                    add = ", ";
                }
            } // FOR
            ret += ")";
            return (ret);
        }
    }
    
    public IndexPlan(Database catalog_db) {
        for (Table catalog_tbl : catalog_db.getTables()) {
            this.put(catalog_tbl, new HashSet<IndexPlan.Entry>());
        } // FOR
    }
    
    /**
     * 
     * @return
     */
    public Set<Entry> getIndexes() {
        Set<Entry> ret = new HashSet<Entry>();
        for (Table catalog_tbl : this.keySet()) {
            ret.addAll(this.get(catalog_tbl));
        } // FOR
        return (ret);
    }
    
    /**
     * 
     * @return
     */
    public List<Entry> getSortedIndexes() {
        SortedMap<Double, Set<Entry>> sorted = new TreeMap<Double, Set<Entry>>(java.util.Collections.reverseOrder());
        for (Table catalog_tbl : this.keySet()) {
            for (Entry index : this.get(catalog_tbl)) {
                Double weight = index.getWeight();
                if (!sorted.containsKey(weight)) {
                    sorted.put(weight, new HashSet<Entry>());
                }
                sorted.get(weight).add(index);
            } // FOR
        } // FOR
        
        List<Entry> ret = new ArrayList<Entry>();
        for (Double weight : sorted.keySet()) {
            ret.addAll(sorted.get(weight));
        } // FOR
        return (ret);
    }

}

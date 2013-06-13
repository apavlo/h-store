/***************************************************************************
 *   Copyright (C) 2009 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import edu.brown.utils.AbstractTreeWalker;

/**
 * @author pavlo
 */
public class DependencyUtil {
    private static final Logger LOG = Logger.getLogger(DependencyUtil.class);

    /**
     * For each table, we store an ordered list tables up to the root of the
     * schema tree Table -> Set
     * <Table>
     */
    private final Map<String, Set<String>> table_ancestors = new HashMap<String, Set<String>>();

    /**
     * For each table we store a set of the tables that are dependent to it
     * Table -> Set
     * <Table>
     */
    private final Map<String, Set<String>> table_descendants = new HashMap<String, Set<String>>();

    /**
     * For each column, we store an ordered list of columns up to the root of
     * the schema tree Table.Column -> Vector
     * <Table.Column>
     */
    private final Map<String, List<String>> column_ancestors = new HashMap<String, List<String>>();

    /**
     * For each column we store a set of the columns that are dependent to it
     * Table.Column -> Set
     * <Table.Column>
     */
    private final Map<String, Set<String>> column_descendants = new HashMap<String, Set<String>>();

    private static final Map<Database, DependencyUtil> singletons = new HashMap<Database, DependencyUtil>();

    /**
     * Return a singleton instance for the given catalog
     * 
     * @param catalog_db
     * @return
     */
    public static DependencyUtil singleton(Database catalog_db) {
        DependencyUtil ret = DependencyUtil.singletons.get(catalog_db);
        if (ret == null) {
            ret = new DependencyUtil(catalog_db);
            DependencyUtil.singletons.put(catalog_db, ret);
        }
        return (ret);
    }

    /**
     * Constructor
     * 
     * @param catalog_db
     */
    private DependencyUtil(Database catalog_db) {
        this.init(catalog_db);
    }

    /**
     * Initialize the various data structures that we will use
     */
    private void init(final Database catalog_db) {
        // Build the paths from columns to their ancestors based on foreign keys
        for (Table catalog_tbl : catalog_db.getTables()) {
            assert (catalog_tbl != null);
            final Set<String> tbl_ancestors = new LinkedHashSet<String>();
            String table_key = CatalogKey.createKey(catalog_tbl);
            this.table_descendants.put(table_key, new HashSet<String>());

            // Columns
            // LOG.info("Getting foreign key dependencies for " + catalog_tbl +
            // "\n" + CatalogUtil.getForeignKeyDependents(catalog_tbl));
            for (Column catalog_col : CatalogUtil.getForeignKeyDependents(catalog_tbl)) {
                assert (catalog_col != null);
                final List<String> col_ancestors = new ArrayList<String>();

                new AbstractTreeWalker<Column>() {
                    protected void populate_children(AbstractTreeWalker.Children<Column> children, Column element) {
                        // For the current element, we need to look at it's
                        // parents in the
                        // DependencyGraph and select the one that our foreign
                        // key column points to
                        Column catalog_fkey_col = CatalogUtil.getForeignKeyParent(element);
                        if (catalog_fkey_col != null)
                            children.addAfter(catalog_fkey_col);
                    }

                    @Override
                    protected void callback(Column element) {
                        if (element != this.getFirst()) {
                            col_ancestors.add(CatalogKey.createKey(element));
                            tbl_ancestors.add(CatalogKey.createKey(element.getParent()));
                        }
                    }
                }.traverse(catalog_col);
                this.column_ancestors.put(CatalogKey.createKey(catalog_col), col_ancestors);
            } // FOR
            this.table_ancestors.put(table_key, tbl_ancestors);
        } // FOR
        // Build a reverse index so that we know all the columns that depend
        // somewhere down the line for each column
        for (String key : this.table_ancestors.keySet()) {
            for (String ancestor_key : this.table_ancestors.get(key)) {
                this.table_descendants.get(ancestor_key).add(key);
            } // FOR
        } // FOR
        for (String key : this.column_ancestors.keySet()) {
            for (String ancestor_key : this.column_ancestors.get(key)) {
                if (!this.column_descendants.containsKey(ancestor_key)) {
                    this.column_descendants.put(ancestor_key, new HashSet<String>());
                }
                this.column_descendants.get(ancestor_key).add(key);
            } // FOR
        } // FOR
    }

    /**
     * Return an unordered set all the foreign key ancestor tables for the given table
     * @param catalog_tbl
     */
    public Collection<Table> getAncestors(Table catalog_tbl) {
        Database catalog_db = (Database) catalog_tbl.getParent();
        Set<Table> ret = new LinkedHashSet<Table>();
        String key = CatalogKey.createKey(catalog_tbl);
        for (String ancestor_key : this.table_ancestors.get(key)) {
            // If this table is missing from the catalog, then we want to stop
            // the ancestor list
            Table ancestor_tbl = CatalogKey.getFromKey(catalog_db, ancestor_key, Table.class);
            if (ancestor_tbl == null)
                break;
            // Otherwise, add it to our list
            ret.add(ancestor_tbl);
        } // FOR
        return (ret);
    }

    /**
     * Return an ordered list of all the foreign ancestor columns for the given column.
     * @param catalog_col
     */
    public List<Column> getAncestors(Column catalog_col) {
        Database catalog_db = (Database) catalog_col.getParent().getParent();
        List<Column> ret = new ArrayList<Column>();
        String key = CatalogKey.createKey(catalog_col);
        for (String ancestor_key : this.column_ancestors.get(key)) {
            // If this table is missing from the catalog, then we want to stop
            // the ancestor list
            Column ancestor_col = CatalogKey.getFromKey(catalog_db, ancestor_key, Column.class);
            if (ancestor_col == null)
                break;
            // Otherwise, add it to our list
            ret.add(ancestor_col);
        } // FOR
        return (ret);
    }

    /**
     * Return an unordered set of foreign key descendant tables for the given table
     * @param catalog_tbl
     * @return
     */
    public Collection<Table> getDescendants(Table catalog_tbl) {
        Database catalog_db = (Database) catalog_tbl.getParent();
        Set<Table> ret = new HashSet<Table>();
        String key = CatalogKey.createKey(catalog_tbl);
        boolean contains = this.table_descendants.containsKey(key);
        if (contains == false) {
            LOG.warn("Missing " + key + "???");
            LOG.warn(this.debug());
            // System.out.println(this.table_descendants.keySet());
            LOG.warn(CatalogUtil.debug(catalog_db.getTables()));
        }
        assert (contains) : "No table descendants for " + key + " (" + contains + ")";
        for (String dependent_key : this.table_descendants.get(key)) {
            Table dependent_tbl = CatalogKey.getFromKey(catalog_db, dependent_key, Table.class);
            // If the table is missing, that's ok...
            if (dependent_tbl != null)
                ret.add(dependent_tbl);
        } // FOR
        return (ret);
    }

    /**
     * Return an unordered set of foreign key descendant columns for the given column
     * @param catalog_col
     * @return
     */
    public Collection<Column> getDescendants(Column catalog_col) {
        Database catalog_db = (Database) catalog_col.getParent().getParent();
        Set<Column> ret = new HashSet<Column>();
        String key = CatalogKey.createKey(catalog_col);
        if (this.column_descendants.containsKey(key)) {
            for (String dependent_key : this.column_descendants.get(key)) {
                Column dependent_col = CatalogKey.getFromKey(catalog_db, dependent_key, Column.class);
                // If the table is missing, that's ok...
                if (dependent_col != null)
                    ret.add(dependent_col);
            } // FOR
        }
        return (ret);
    }

    public String debug() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("Descendants:\n");
        for (String k : this.table_descendants.keySet()) {
            buffer.append(k).append(": ").append(this.table_descendants.get(k)).append("\n");
        }
        buffer.append("---------------------------------------\n");
        buffer.append("Ancestors:\n");
        for (String k : this.table_ancestors.keySet()) {
            buffer.append(k).append(": ").append(this.table_ancestors.get(k)).append("\n");
        }
        return (buffer.toString());
    }
}

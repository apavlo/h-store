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
package edu.brown.designer.mappers;

import org.apache.log4j.Logger;

import edu.brown.designer.Designer;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.partitioners.plan.PartitionPlan;

public abstract class AbstractMapper {
    protected static final Logger LOG = Logger.getLogger(AbstractMapper.class.getName());

    protected final Designer designer;
    protected final DesignerInfo info;

    public AbstractMapper(Designer designer, DesignerInfo info) {
        this.designer = designer;
        this.info = info;
    }

    /**
     * @param ptrees
     * @return
     * @throws Exception
     */
    public abstract PartitionMapping generate(DesignerHints hints, PartitionPlan pplan) throws Exception;

    /**
     * @param plan
     * @throws Exception
     */
    /*
     * public void calculateProcedurePartitioning(PartitionPlan plan) throws
     * Exception { Set<Table> roots = plan.getRoots(); // // Go through each
     * procedure and try to figure out what the table.column it should be
     * "partitioned" on // PartitionTree ptree = plan.getPartitionTree(); for
     * (Procedure catalog_proc : this.catalog_db.getProcedures()) {
     * System.out.println("Calculating partition attribute for " +
     * catalog_proc); Set<Table> proc_tables =
     * CatalogUtil.getTables(catalog_proc); System.out.println(catalog_proc +
     * " Tables -> " + proc_tables); Map<Table, Integer> partition_col_counts =
     * new HashMap<Table, Integer>(); // // For each table, get the root of the
     * tree that contain this table in the // PartitionTree and add up the
     * totals for the partitioning columns // for (Table catalog_tbl :
     * proc_tables) { PartitionEntry<Table> entry =
     * plan.getTablePartitions().get(catalog_tbl); if (entry == null) {
     * LOG.fatal("No PartitionEntry exists for " + catalog_tbl + " in " +
     * catalog_proc); continue; } if (entry.getMethod() !=
     * PartitionMethodType.REPLICATION) { for (Table catalog_root : roots) { if
     * (ptree.getPath(ptree.getVertex(catalog_root),
     * ptree.getVertex(catalog_tbl)).isEmpty()) { int cnt = 1; if
     * (partition_col_counts.containsKey(catalog_root)) { cnt =
     * partition_col_counts.get(catalog_root); }
     * partition_col_counts.put(catalog_root, cnt); break; } } // FOR } } // FOR
     * // // If there were no partitioning attributes found, then what do we
     * do??? // if (partition_col_counts.isEmpty()) { } // // Pick the one with
     * the greatest value // Table partition_table =
     * CollectionUtil.getGreatest(partition_col_counts); PartitionEntry<Table>
     * tbl_entry = plan.getTablePartitions().get(partition_table); if (tbl_entry
     * != null) { System.out.println(catalog_proc + ": " + tbl_entry);
     * PartitionEntry<Procedure> proc_entry = new
     * PartitionEntry<Procedure>(catalog_proc, PartitionMethodType.HASH,
     * tbl_entry.getAttributes()); // // If there is more than one partition
     * column, then it is not single sited //
     * proc_entry.setSingleSited(partition_col_counts.size() > 1);
     * plan.getProcedurePartitions().put(catalog_proc, proc_entry); } } // FOR
     * return; }
     */
}

/**
 * 
 */
package edu.brown.hashing;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.CatalogContext;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.hstore.HStoreConstants;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.utils.FileUtil;
import edu.brown.utils.JSONSerializable;


//       TODO This class likely needs to be relocated (ae)
/**
 * @author aelmore A container for statically defined partitions plans. Each
 *         plan will contain multiple partition phases. Each partition phase
 *         will contain a list of tables that dictate how the table is
 *         partitioned. <br>
 *         PlannedPartitions Hierarchy:
 *         <ul>
 *          <li> Map[String, PartitionPhase] partition_phase_map
 *             <ul><li> Map[String, PartitionedTable] tables_map
 *               <ul><li>  List[PartitionRange] partitions
 *                 <ul><li> PartitionRange: min,max,partition_id </ul></ul></ul>
 *         </ul>
 */

public class PlannedPartitions implements JSONSerializable {
  private static final Logger LOG = Logger.getLogger(PlannedPartitions.class);
  private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
  private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
  public static final String PLANNED_PARTITIONS = "partition_plans";
  public static final String TABLES = "tables";
  public static final String PARTITIONS = "partitions";

  static {
    LoggerUtil.attachObserver(LOG, debug, trace);
  }

  private CatalogContext catalog_context;
  private Map<String, VoltType> table_vt_map;
  private Map<String, PartitionPhase> partition_phase_map;
  private ParameterMappingsSet paramMappings;
  private String current_phase;

  public PlannedPartitions(CatalogContext catalog_context, File planned_partition_json_file) throws Exception {
    this(catalog_context, new JSONObject(FileUtil.readFile(planned_partition_json_file)));
  }

  public PlannedPartitions(CatalogContext catalog_context, JSONObject planned_partition_json) throws Exception {
    this.catalog_context = catalog_context;
    this.partition_phase_map = new HashMap<>();

    this.paramMappings = catalog_context.paramMappings;
    // TODO find catalogContext.getParameter mapping to find statement_colum
    // from project mapping (ae)

    table_vt_map = new HashMap<>();
    for (Table table : catalog_context.getDataTables()) {

      VoltType.get(table.getPartitioncolumn().getType());
      table_vt_map.put(table.getName().toLowerCase(), VoltType.BIGINT);
    }

    if (planned_partition_json.has(PLANNED_PARTITIONS)) {
      JSONObject phases = planned_partition_json.getJSONObject(PLANNED_PARTITIONS);

      Iterator<String> keys = phases.keys();
      while (keys.hasNext()) {
        String key = keys.next();
        JSONObject phase = phases.getJSONObject(key);
        partition_phase_map.put(key, new PartitionPhase(catalog_context, table_vt_map, phase));
      }
    } else {
      throw new JSONException(String.format("JSON file is missing key \"%s\". ", PLANNED_PARTITIONS));
    }

  }
  
  /**
   * Get the partition id for a given table and partition id/key
   * @param table_name
   * @param id
   * @return the partition id, or -1 / null partition if the id/key is not found in the plan
   * @throws Exception
   */
  public int getPartitionId(String table_name, Object id) throws Exception {
    return partition_phase_map.get(getCurrent_phase()).getTable(table_name).findPartition(id);
  }

  

  // ******** Containers *****************************************/

  /**
   * @author aelmore Holds the phases/epochs/version of a partition plan
   */
  public static class PartitionPhase {
    protected Map<String, PartitionedTable<? extends Comparable<?>>> tables_map;

    @SuppressWarnings("unchecked")
    public List<PartitionRange<? extends Comparable<?>>> getPartitions(String table_name) {
      return (List<PartitionRange<? extends Comparable<?>>>) tables_map.get(table_name);
    }

    public PartitionedTable<? extends Comparable<?>> getTable(String table_name) {
      return tables_map.get(table_name);
    }

    /**
     * Create a new partition phase
     * 
     * @param catalog_db
     * @param table_vt_map
     *          mapping of table names to volt type of partition col
     * @param phase
     *          JSONObject
     */
    public PartitionPhase(CatalogContext catalog_context, Map<String, VoltType> table_vt_map, JSONObject phase) throws Exception {
      this.tables_map = new HashMap<String, PlannedPartitions.PartitionedTable<? extends Comparable<?>>>();
      assert (phase.has(TABLES));
      JSONObject json_tables = phase.getJSONObject(TABLES);
      Iterator<String> table_names = json_tables.keys();
      while (table_names.hasNext()) {
        String table_name = table_names.next();
        assert (table_vt_map.containsKey(table_name.toLowerCase()));
        JSONObject table_json = json_tables.getJSONObject(table_name.toLowerCase());
        // Class<?> c = table_vt_map.get(table_name).classFromType();
        tables_map.put(table_name, new PartitionedTable<>(table_vt_map.get(table_name), table_name, table_json));
      }

    }
  }

  /**
   * @author aelmore Holds the partitioning for a table, during a given phase
   * @param <T>
   *          The type of the ID which is partitioned on. Comparable
   */
  public static class PartitionedTable<T extends Comparable<T>> {
    protected List<PartitionRange<T>> partitions;
    private String table_name;
    private VoltType vt;

    public PartitionedTable(VoltType vt, String table_name, JSONObject table_json) throws Exception {
      this.partitions = new ArrayList<>();
      this.table_name = table_name;
      this.vt = vt;
      assert (table_json.has(PARTITIONS));
      JSONObject partitions_json = table_json.getJSONObject(PARTITIONS);
      Iterator<String> partitions = partitions_json.keys();
      while (partitions.hasNext()) {
        String partition = partitions.next();
        // TODO do we need more than ints, what about specifying ranges as
        // replicated tables (ae)
        int partition_id = Integer.parseInt(partition);
        addPartitionRanges(partition_id, partitions_json.getString(partition));
      }
    }

    /**
     * Find the partition for a key
     * 
     * @param id
     * @return the partition id or null partition id if no match could be found
     */
    @SuppressWarnings("unchecked")
    public int findPartition(Object id) throws Exception {
      if (trace.val)
        LOG.trace(String.format("Looking up key %s on table %s during phase %s", id, table_name));
      assert (id instanceof Comparable<?>);

      // TODO I am sure there is a better way to do this... Andy? (ae)
      T cast_id = (T) id;

      for (PartitionRange<T> p : partitions) {
        // if this greater than or equal to the min inclusive val and
        // less than
        // max_exclusive or equal to both min and max (singleton)
        if ((p.min_inclusive.compareTo(cast_id) <= 0 && p.max_exclusive.compareTo(cast_id) > 0)
            || (p.min_inclusive.compareTo(cast_id) == 0 && p.max_exclusive.compareTo(cast_id) == 0)) {
          return p.partition;
        }
      }
      return HStoreConstants.NULL_PARTITION_ID;
    }

    /**
     * Associate a partition with a set of values in the form of val or
     * val1,val2 or val1-val2 or val1,val2-val3 or some other combination
     * 
     * @param partition_id
     * @param partitionValues
     * @throws ParseException
     */
    public void addPartitionRanges(int partition_id, String partition_values) throws ParseException {
      for (String range : partition_values.split(",")) {
        partitions.add(new PartitionRange<T>(vt, partition_id, range));
      }
    }
  }

  /**
   * @author aelmore A defined range of keys and an associated partition id
   * @param <T>
   *          Comparable type of key
   */
  public static class PartitionRange<T extends Comparable<?>> {
    protected T min_inclusive;
    protected T max_exclusive;
    protected int partition;
    protected VoltType vt;

    @SuppressWarnings("unchecked")
    public PartitionRange(VoltType vt, int partition_id, String range) throws ParseException {
      this.vt = vt;
      this.partition = partition_id;

      // TODO add support for open ranges ie -100 (< 100) and 100- (anything >=
      // 100)

      // x-y
      if (range.contains("-")) {
        String vals[] = range.split("-", 2);
        Object min_obj = VoltTypeUtil.getObjectFromString(vt, vals[0]);
        min_inclusive = (T) min_obj;
        Object max_obj = VoltTypeUtil.getObjectFromString(vt, vals[1]);
        max_exclusive = (T) max_obj;
      }
      // x
      else {
        Object obj = VoltTypeUtil.getObjectFromString(vt, range);
        min_inclusive = (T) obj;
        max_exclusive = (T) obj;
      }

    }

    @Override
    public String toString() {
      return "PartitionRange [" + min_inclusive + "-" + max_exclusive + ") p_id=" + partition + "]";
    }

  }

  // ********End Containers **************************************/


  /**
   * Update the current partition phase (plan/epoch/etc)
   * @param phase
   */
  public synchronized void setPartitionPhase(String phase) {
    this.current_phase = phase;
  }

  /**
   * @return the current partition phase/epoch
   */
  public synchronized String getCurrent_phase() {
    return current_phase;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.json.JSONString#toJSONString()
   */
  @Override
  public String toJSONString() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.brown.utils.JSONSerializable#save(java.io.File)
   */
  @Override
  public void save(File output_path) throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.brown.utils.JSONSerializable#load(java.io.File,
   * org.voltdb.catalog.Database)
   */
  @Override
  public void load(File input_path, Database catalog_db) throws IOException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.brown.utils.JSONSerializable#toJSON(org.json.JSONStringer)
   */
  @Override
  public void toJSON(JSONStringer stringer) throws JSONException {
    // TODO Auto-generated method stub

  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.brown.utils.JSONSerializable#fromJSON(org.json.JSONObject,
   * org.voltdb.catalog.Database)
   */
  @Override
  public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
    // TODO Auto-generated method stub

  }

}

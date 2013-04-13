/**
 * 
 */
package edu.brown.hashing;

import java.io.File;
import java.io.IOException;
import java.security.KeyException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.utils.VoltTypeUtil;
import org.voltdb.utils.VoltUncaughtExceptionHandler;

import edu.brown.BaseTestCase;
import edu.brown.hstore.HStoreConstants;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.JSONSerializable;

/**
 * @author aelmore A container for statically defined partitions plans. Each
 *         plan will contain multiple partition phases. Each partition phase
 *         will contain a list of tables that dictate how the table is
 *         partitioned. TODO This class likely needs to be relocated (ae)
 * 
 */

public class PlannedPartitions implements JSONSerializable {
  private static final Logger LOG = Logger.getLogger(PlannedPartitions.class);
  private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
  private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
  static {
    LoggerUtil.attachObserver(LOG, debug, trace);
  }

  private Database catalog_db;
  private Map<String, VoltType> table_vt_map;

  public PlannedPartitions(Database catalog_db) {
    this.catalog_db = catalog_db;
    // TODO find catalogContext.getParameter mapping to find statement_colum
    // from project mapping
    
    table_vt_map = new HashMap<>();
    for (Table table : catalog_db.getTables()) {
      // LEFTOFF TODO
      
    }
    // VoltType vt = VoltType.get(catalog_param.getType());
    //
  }

  // ******** Containers *****************************************/

  /**
   * @author aelmore Holds the phases/epochs/version of a partition plan
   */
  public static class PartitionPhase {
    protected Map<String, TablePartitions<? extends Comparable<?>>> table_partitions;

    @SuppressWarnings("unchecked")
    public List<PartitionRange<? extends Comparable<?>>> getPartitions(String table_name) {
      return (List<PartitionRange<? extends Comparable<?>>>) table_partitions.get(table_name);
    }

    public PartitionPhase(Database catalog_db) {
      this.table_partitions = new HashMap<String, PlannedPartitions.TablePartitions<? extends Comparable<?>>>();
    }

    public void addTablePartition(TablePartitions<? extends Comparable<?>> partitioning) {
      this.table_partitions.put(partitioning.table_name, partitioning);
    }
  }

  /**
   * @author aelmore Holds the partitioning for a table, during a given phase
   * @param <T>
   *          The type of the ID which is partitioned on. Comparable
   */
  public static class TablePartitions<T extends Comparable<T>> {
    protected List<PartitionRange<T>> partitions;
    private String table_name;
    private VoltType vt;

    public TablePartitions(VoltType vt, String table_name) {
      this.partitions = new ArrayList<>();
      this.table_name = table_name;
      this.vt = vt;
    }

    /**
     * Find the partition for a key
     * 
     * @param key_value
     * @return the partition id or null partition id if no match could be found
     */
    public int findPartition(T key_value) throws Exception {
      if (trace.val)
        LOG.trace(String.format("Looking up key %s on table %s during phase %s", key_value, table_name));

      for (PartitionRange<T> p : partitions) {
        // if this greater than or equal to the min inclusive val and less than
        // max_exclusive or equal to both min and max (singleton)
        if ((p.min_inclusive.compareTo(key_value) <= 0 && p.max_exclusive.compareTo(key_value) > 0)
            || (p.min_inclusive.compareTo(key_value) == 0 && p.max_exclusive.compareTo(key_value) == 0)) {
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
   * 
   * @param <T>
   *          Comparable type of key
   */
  public static class PartitionRange<T extends Comparable<?>> implements Comparable<T> {
    protected T min_inclusive;
    protected T max_exclusive;
    protected int partition;
    protected VoltType vt;

    @SuppressWarnings("unchecked")
    public PartitionRange(VoltType vt, int partition_id, String range) throws ParseException {
      this.vt = vt;

      // x-y
      if (range.contains("-")) {
        String vals[] = range.split("-", 1);
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
    public int compareTo(T o) {
      // TODO Auto-generated method stub
      return 0;
    }
  }

  // ********End Containers **************************************/

  private Map<String, PartitionPhase> partition_phases;
  private String current_phase;

  public synchronized void setPartitionPhase(String phase) {
    this.current_phase = phase;
  }

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

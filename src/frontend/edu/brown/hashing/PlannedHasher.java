/**
 * 
 */
package edu.brown.hashing;

import org.json.JSONObject;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.utils.NotImplementedException;

/**
 * @author aelmore Hasher that uses a planned partition plan, stored in the
 *         database catalog. This partition plan can change over time
 * 
 */
public class PlannedHasher extends DefaultHasher {

  String ycsb_plan = "{"+
      "       \"default_table\":\"usertable\"," +        
      "       \"partition_plans\":{"+
      "          \"1\" : {"+
      "            \"tables\":{"+
      "              \"usertable\":{"+
      "                \"partitions\":{"+
      "                  0 : \"0-100000\""+
      "                }     "+
      "              }"+
      "            }"+
      "          },"+
      "          \"2\" : {"+
      "            \"tables\":{"+
      "              \"usertable\":{"+
      "                \"partitions\":{"+
      "                  0 : \"0-50000\","+
      "                  1 : \"50000-100000\""+
      "                }     "+
      "              }"+
      "            }"+
      "          }"+ 
      "        }"+
      "}";
  
  private PlannedPartitions planned_partitions = null;
  
  public void changePartitionPhase(String partition_plan) throws Exception{
    planned_partitions.setPartitionPhase(partition_plan);
  }
  /**
   * @param catalog_db
   * @param num_partitions
   */
  public PlannedHasher(CatalogContext catalogContext, int num_partitions) {
    super(catalogContext, num_partitions);
    try{
      JSONObject partition_json = new JSONObject(ycsb_plan);
      planned_partitions = new PlannedPartitions(catalogContext,partition_json);
    }catch(Exception ex){
      LOG.error("Error intializing planned partitions", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * @param catalog_db
   */
  public PlannedHasher(CatalogContext catalogContext) {
    super(catalogContext);
  }

  @Override
  public int hash(Object value) {
    throw new NotImplementedException("Hashing without Catalog not supported");
  }

  @Override
  public int hash(Object value, CatalogType catalogItem) {
    if (catalogItem instanceof Column || catalogItem instanceof Procedure || catalogItem instanceof Statement) {
      try {
        return planned_partitions.getPartitionId(catalogItem, value);
      } catch (Exception e) {
        LOG.error("Error on looking up partitionId from planned partition", e);
        throw new RuntimeException(e);
      }
    } 
    throw new NotImplementedException("TODO");
  }

  @Override
  public int hash(Object value, int num_partitions) {
    throw new NotImplementedException("Hashing without Catalog not supported");
  }

}

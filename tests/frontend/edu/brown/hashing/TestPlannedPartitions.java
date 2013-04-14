package edu.brown.hashing;

import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;

public class TestPlannedPartitions extends BaseTestCase {

  public TestPlannedPartitions() {
    
  }
  
  public String test_json1 = "{"+
  "       \"partition_plans\":{"+
  "          \"1\" : {"+
  "            \"tables\":{"+
  "              \"usertable\":{"+
  "                \"partitions\":{"+
  "                  1 : \"1-100\","+
  "                  2 : \"100-300\","+
  "                  3 : \"300,350-400,302\","+
  "                  4 : \"301,303,304-350\"       "+
  "                }     "+
  "              }"+
  "            }"+
  "          }"+
  "        }"+
  "}";

  @Override
  protected void setUp() throws Exception {
      super.setUp(ProjectType.YCSB);
      Table catalog_tbl = this.getTable("USERTABLE");
      Column catalog_col = this.getColumn(catalog_tbl, "YCSB_KEY");
      catalog_tbl.setPartitioncolumn(catalog_col);
  }
  
  public void testBuildTablePartitions() throws Exception
  {
    JSONObject test_json = new JSONObject(test_json1);
    PlannedPartitions p = new PlannedPartitions(catalogContext,test_json);
    p.setPartitionPhase("1");
    // assertEquals(true,false);//TODO leftoff, write a function to get a partition id for a table and key
  }

}

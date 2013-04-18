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
  "          },"+
  "          \"2\" : {"+
  "            \"tables\":{"+
  "              \"usertable\":{"+
  "                \"partitions\":{"+
  "                  1 : \"1-400\","+
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

  public void testBuildTablePartitions() throws Exception {
    JSONObject test_json = new JSONObject(test_json1);
    PlannedPartitions p = new PlannedPartitions(catalogContext, test_json);
    p.setPartitionPhase("1");
    assertEquals(1, p.getPartitionId("usertable", new Long(2)));
    assertEquals(1, p.getPartitionId("usertable", new Long(1)));
    assertEquals(1, p.getPartitionId("usertable", new Long(99)));
    assertEquals(2, p.getPartitionId("usertable", new Long(100)));
    assertEquals(2, p.getPartitionId("usertable", new Long(157)));
    assertEquals(2, p.getPartitionId("usertable", new Long(299)));
    assertEquals(3, p.getPartitionId("usertable", new Long(300)));
    assertEquals(3, p.getPartitionId("usertable", new Long(350)));
    assertEquals(3, p.getPartitionId("usertable", new Long(399)));
    assertEquals(3, p.getPartitionId("usertable", new Long(302)));
    assertEquals(4, p.getPartitionId("usertable", new Long(301)));
    assertEquals(4, p.getPartitionId("usertable", new Long(303)));
    assertEquals(4, p.getPartitionId("usertable", new Long(304)));
    assertEquals(4, p.getPartitionId("usertable", new Long(340)));
    assertEquals(-1, p.getPartitionId("usertable", new Long(0)));
    assertEquals(-1, p.getPartitionId("usertable", new Long(54521)));

    p.setPartitionPhase("2");
    assertEquals(1, p.getPartitionId("usertable", new Long(2)));
    assertEquals(1, p.getPartitionId("usertable", new Long(1)));
    assertEquals(1, p.getPartitionId("usertable", new Long(99)));
    assertEquals(1, p.getPartitionId("usertable", new Long(100)));
    assertEquals(1, p.getPartitionId("usertable", new Long(157)));
    assertEquals(1, p.getPartitionId("usertable", new Long(299)));
    assertEquals(1, p.getPartitionId("usertable", new Long(300)));
    assertEquals(1, p.getPartitionId("usertable", new Long(350)));
    assertEquals(1, p.getPartitionId("usertable", new Long(399)));
    assertEquals(1, p.getPartitionId("usertable", new Long(302)));
    assertEquals(1, p.getPartitionId("usertable", new Long(301)));
    assertEquals(1, p.getPartitionId("usertable", new Long(303)));
    assertEquals(1, p.getPartitionId("usertable", new Long(304)));
    assertEquals(1, p.getPartitionId("usertable", new Long(340)));
    assertEquals(-1, p.getPartitionId("usertable", new Long(0)));
    assertEquals(-1, p.getPartitionId("usertable", new Long(54521)));
  }

}

package edu.brown.hashing;

import java.io.File;
import java.text.ParseException;

import org.json.JSONObject;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.hashing.PlannedPartitions.PartitionRange;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;

public class TestPlannedPartitions extends BaseTestCase {

  public TestPlannedPartitions() {

  }

  public String test_json1 = "{" + "       \"default_table\":\"usertable\"," + "       \"partition_plans\":{" + "          \"1\" : {"
      + "            \"tables\":{" + "              \"usertable\":{" + "                \"partitions\":{"
      + "                  1 : \"1-100\"," + "                  2 : \"100-300\"," + "                  3 : \"300,350-400,302\","
      + "                  4 : \"301,303,304-350\"       " + "                }     " + "              }" + "            }"
      + "          }," + "          \"2\" : {" + "            \"tables\":{" + "              \"usertable\":{"
      + "                \"partitions\":{" + "                  1 : \"1-400\"," + "                }     " + "              }"
      + "            }" + "          }" + "        }" + "}";
  private File json_path;

  @Override
  protected void setUp() throws Exception {
    super.setUp(ProjectType.YCSB);
    Table catalog_tbl = this.getTable("USERTABLE");
    Column catalog_col = this.getColumn(catalog_tbl, "YCSB_KEY");
    catalog_tbl.setPartitioncolumn(catalog_col);
    String tmp_dir = System.getProperty("java.io.tmpdir");
    json_path = FileUtil.join(tmp_dir, "test1.json");
    FileUtil.writeStringToFile(json_path, test_json1);
  }

  public void testReadJSON() throws Exception {
    File f = new File(json_path.getAbsolutePath());
    assertNotNull(f);
    assert (f.exists());
    JSONObject test_json = new JSONObject(FileUtil.readFile(f));
    PlannedPartitions p = new PlannedPartitions(catalogContext, test_json);
    p.setPartitionPhase("1");
    assertEquals(1, p.getPartitionId("usertable", new Long(99)));
    assertEquals(2, p.getPartitionId("usertable", new Long(100)));
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

  public void testPartitionRangeCompare() throws Exception {

    PartitionRange<Integer> pr1_4 = new PartitionRange<Integer>(VoltType.INTEGER, 1, "1-4");
    PartitionRange<Integer> pr1_4b = new PartitionRange<Integer>(VoltType.INTEGER, 1, "1-4");
    PartitionRange<Integer> pr1_20 = new PartitionRange<Integer>(VoltType.INTEGER, 1, "1-20");
    PartitionRange<Integer> pr2_3 = new PartitionRange<Integer>(VoltType.INTEGER, 1, "2-3");
    PartitionRange<Integer> pr2_4 = new PartitionRange<Integer>(VoltType.INTEGER, 1, "2-4");
    PartitionRange<Integer> pr3_3 = new PartitionRange<Integer>(VoltType.INTEGER, 1, "3-3");
    PartitionRange<Integer> pr20_300 = new PartitionRange<Integer>(VoltType.INTEGER, 1, "20-300");
    PartitionRange<Integer> pr40_50 = new PartitionRange<Integer>(VoltType.INTEGER, 1, "40-50");
    boolean exceptionCaught = false;
    try {
      PartitionRange<Integer> pr5_3 = new PartitionRange<Integer>(VoltType.INTEGER, 1, "5-3");
    } catch (ParseException ex) {
      exceptionCaught = true;
    }
    assertTrue(exceptionCaught);
    assertTrue(pr1_4.compareTo(pr1_4b) == 0);
    assertTrue(pr1_4.compareTo(pr1_20) < 0);
    assertTrue(pr1_4.compareTo(pr2_3) < 0);
    assertTrue(pr2_3.compareTo(pr1_4) > 0);
    assertTrue(pr2_3.compareTo(pr2_4) < 0);
    assertTrue(pr2_3.compareTo(pr3_3) < 0);
    assertTrue(pr2_4.compareTo(pr3_3) < 0);
    assertTrue(pr20_300.compareTo(pr40_50) < 0);
    assertTrue(pr40_50.compareTo(pr20_300) > 0);
    assertTrue(pr40_50.compareTo(pr2_4) > 0);

  }

}

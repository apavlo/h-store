package edu.brown.hashing;

import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.hashing.PlannedPartitions.PartitionRange;
import edu.brown.hashing.PlannedPartitions.PartitionedTable;
import edu.brown.hashing.PlannedPartitions.ReconfigurationRange;
import edu.brown.hashing.PlannedPartitions.ReconfigurationTable;
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

  public void testReconfigurationTable1() throws Exception {
    List<PartitionRange<Integer>> olds = new ArrayList<>();
    List<PartitionRange<Integer>> news = new ArrayList<>();

    PartitionRange<Integer> o1_10 = new PartitionRange<Integer>(VoltType.INTEGER, 1, "1-10");
    PartitionRange<Integer> o10_20 = new PartitionRange<Integer>(VoltType.INTEGER, 2, "10-20");
    PartitionRange<Integer> o20_30 = new PartitionRange<Integer>(VoltType.INTEGER, 3, "20-30");
    olds.add(o1_10);
    olds.add(o10_20);
    olds.add(o20_30);
    PartitionedTable<Integer> old_table = new PartitionedTable<>(olds, "table", VoltType.INTEGER);

    PartitionRange<Integer> n1_5 = new PartitionRange<Integer>(VoltType.INTEGER, 1, "1-5");
    PartitionRange<Integer> n5_7 = new PartitionRange<Integer>(VoltType.INTEGER, 2, "5-7");
    PartitionRange<Integer> n7_10 = new PartitionRange<Integer>(VoltType.INTEGER, 3, "7-10");
    PartitionRange<Integer> n10_25 = new PartitionRange<Integer>(VoltType.INTEGER, 2, "10-25");
    PartitionRange<Integer> n25_26 = new PartitionRange<Integer>(VoltType.INTEGER, 1, "25-26");
    PartitionRange<Integer> n26_30 = new PartitionRange<Integer>(VoltType.INTEGER, 3, "26-30");
    news.add(n1_5);
    news.add(n5_7);
    news.add(n7_10);
    news.add(n10_25);
    news.add(n25_26);
    news.add(n26_30);
    PartitionedTable<Integer> new_table = new PartitionedTable<>(news, "table", VoltType.INTEGER);

    ReconfigurationTable<Integer> reconfig = new ReconfigurationTable<>(old_table, new_table);
    ReconfigurationRange<Integer> range = null;
    range = reconfig.reconfigurations.get(0);
    assertTrue(range.min_inclusive == 5 && range.max_exclusive == 7 && range.old_partition == 1 && range.new_partition == 2);

    range = reconfig.reconfigurations.get(1);
    assertTrue(range.min_inclusive == 7 && range.max_exclusive == 10 && range.old_partition == 1 && range.new_partition == 3);

    range = reconfig.reconfigurations.get(2);
    assertTrue(range.min_inclusive == 20 && range.max_exclusive == 25 && range.old_partition == 3 && range.new_partition == 2);

    range = reconfig.reconfigurations.get(3);
    assertTrue(range.min_inclusive == 25 && range.max_exclusive == 26 && range.old_partition == 3 && range.new_partition == 1);

  }
}

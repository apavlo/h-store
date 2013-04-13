package edu.brown.hashing;

import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public class TestPlannedPartitions extends BaseTestCase {

  public TestPlannedPartitions() {
    
  }
  
  @Override
  protected void setUp() throws Exception {
      super.setUp(ProjectType.YCSB);
      
      Table catalog_tbl = this.getTable("USERTABLE");
      Column catalog_col = this.getColumn(catalog_tbl, "YCSB_KEY");
      catalog_tbl.setPartitioncolumn(catalog_col);
  }
  
  public void testBuildTablePartitions()
  {
    
  }

}

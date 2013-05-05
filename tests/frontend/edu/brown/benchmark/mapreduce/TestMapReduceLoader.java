package edu.brown.benchmark.mapreduce;

import java.lang.reflect.Field;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.ProjectType;

public class TestMapReduceLoader extends BaseTestCase {
    private static final Logger LOG = Logger.getLogger(TestMapReduceLoader.class);
    
    protected static final double SCALE_FACTOR = 0.03;
    
    protected MapReduceLoader loader;
    protected Long current_tablesize;
    protected Long current_batchsize;
    protected Long total_rows = 0l;

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.MAPREDUCE);
        
        String args[] = {
            "client.scalefactor=" + SCALE_FACTOR,
            "NOCONNECTIONS=true",
            "NOUPLOADING=true",
        };
        this.loader = new MapReduceLoader(args) {
            
            @Override
            public ClientResponse loadVoltTable(String tablename, VoltTable table) {
                LOG.info("LOAD TABLE: " + tablename + " [" +
                          "tablesize="  + TestMapReduceLoader.this.current_tablesize + "," +
                          "batchsize="  + TestMapReduceLoader.this.current_batchsize + "," +
                          "num_rows="   + table.getRowCount() + "," + 
                          "total_rows=" + TestMapReduceLoader.this.total_rows + "]");
                assertNotNull("Got null VoltTable object for table '" + tablename + "'", table);
                
                // Simple checks
                int num_rows = table.getRowCount();
                TestMapReduceLoader.this.total_rows += num_rows;
                assert(num_rows > 0);
                assert(num_rows <= TestMapReduceLoader.this.current_batchsize);
                assert(TestMapReduceLoader.this.total_rows <= TestMapReduceLoader.this.current_tablesize) :
                    String.format("%d <= %d", total_rows, current_tablesize);
                
                // VARCHAR Column checks
                Table catalog_tbl = TestMapReduceLoader.this.getTable(tablename);
                table.resetRowPosition();
                while (table.advanceRow()) {
                    int row = table.getActiveRowIndex();
                    for (Column catalog_col : catalog_tbl.getColumns()) {
                        int index = catalog_col.getIndex();
                        VoltType col_type = VoltType.get(catalog_col.getType()); 
                        switch (col_type) {
                            case TINYINT:
                            case SMALLINT:
                            case INTEGER: {
                                // TODO
                                break;
                            }
                            case BIGINT: {
                                long value = table.getLong(index);
                                assert(value < Long.MAX_VALUE);
                                break;
                            }
                            case STRING: {
                                Cluster catalog_clus = CatalogUtil.getCluster(catalog);
//                              Host catalog_host = catalog_clus.getHosts().get("XYZ");
//                              List<Site> catalog_sites = CatalogUtil.getSitesForHost(catalog_host);
//                              catalog_sites.get(0).getPartitions();
//                              int a_id = 20;
//                              TheHashinator.hashToPartition(a_id, catalog_clus.getNum_partitions());
                                int length = catalog_col.getSize();
                                String value = table.getString(index);
                                assertNotNull("The value in " + catalog_col + " at row " + row + " is null", value);
                                assertTrue("The value in " + catalog_col + " at row " + row + " is " + value.length() + ". Max is " + length,
                                           value.length() <= length);
                                break;
                            }
                            case TIMESTAMP: {
                                // TODO
                                break;
                            }
                            default:
                                assert(false) :
                                    "Unexpected type " + col_type + " for column " + catalog_col.getName() + " for row " + row;
                        } // SWITCH

                        
                    } // FOR
                } // WHILE
                // if (true || tablename.equals(MapReduceConstants.TABLENAME_TABLEB)) LOG.info(table);
                table.resetRowPosition();
                return (null);
            }
            @Override
            public CatalogContext getCatalogContext() {
                return BaseTestCase.catalogContext;
            }
        };
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        // assert(this.total_rows == this.current_tablesize);
    }
    
    protected void setCurrentTable(String tablename) throws Exception {
        LOG.debug("Retrieving attributes for table '" + tablename + "'");
        String field_name = "TABLESIZE_" + tablename;
        Field field_handle = MapReduceConstants.class.getField(field_name);
        assertNotNull(field_handle);
        this.current_tablesize = Math.round((Long)field_handle.get(null) * SCALE_FACTOR);

        field_name = "BATCHSIZE_" + tablename;
        field_handle = MapReduceConstants.class.getField(field_name);
        assertNotNull(field_handle);
        this.current_batchsize = (Long)field_handle.get(null);
        
        this.total_rows = 0l;
    }
    
    /**
     * testGenerateTABLEA
     */
    public void testGenerateTABLEA() throws Exception {
        this.setCurrentTable(MapReduceConstants.TABLENAME_TABLEA);
        this.loader.generateTableData(MapReduceConstants.TABLENAME_TABLEA);
    }

    /**
     * testGenerateTABLEB
     */
    public void testGenerateTABLEB() throws Exception {
        this.setCurrentTable(MapReduceConstants.TABLENAME_TABLEB);
//        this.loader.generateTableData(MapReduceConstants.TABLENAME_TABLEB);
    }
 }

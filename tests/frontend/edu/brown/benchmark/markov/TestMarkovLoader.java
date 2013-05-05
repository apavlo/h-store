package edu.brown.benchmark.markov;

import java.lang.reflect.Field;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public class TestMarkovLoader extends BaseTestCase {
    private static final Logger LOG = Logger.getLogger(TestMarkovLoader.class.getSimpleName());

    protected static final double SCALE_FACTOR = 0.01;

    protected MarkovLoader loader;
    protected Long current_tablesize;
    protected Long current_batchsize;
    protected Long total_rows = 0l;

    protected static final String LOADER_ARGS[] = {
        "CLIENT.SCALEFACTOR=" + SCALE_FACTOR,
        "NUMCLIENTS=1", "NOCONNECTIONS=true",
    };

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.MARKOV);

        this.loader = new MarkovLoader(LOADER_ARGS) {
            @Override
            public CatalogContext getCatalogContext() {
                return (BaseTestCase.catalogContext);
            }

            @Override
            protected void loadTable(String tablename, VoltTable table) {
                LOG.debug("LOAD TABLE: " + tablename + " [" + "tablesize=" + TestMarkovLoader.this.current_tablesize + "," + "batchsize=" + TestMarkovLoader.this.current_batchsize + "," + "num_rows="
                        + table.getRowCount() + "," + "total_rows=" + TestMarkovLoader.this.total_rows + "]");
                assertNotNull("Got null VoltTable object for table '" + tablename + "'", table);

                // Simple checks
                int num_rows = table.getRowCount();
                TestMarkovLoader.this.total_rows += num_rows;
                assert (num_rows > 0);
                assert (num_rows <= TestMarkovLoader.this.current_batchsize);
                assert (TestMarkovLoader.this.total_rows <= TestMarkovLoader.this.current_tablesize);

                // VARCHAR Column checks
                Table catalog_tbl = TestMarkovLoader.this.getTable(tablename);
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
                                assert (value < Long.MAX_VALUE);
                                break;
                            }
                            case STRING: {
                                int length = catalog_col.getSize();
                                String value = table.getString(index);
                                assertNotNull("The value in " + catalog_col + " at row " + row + " is null", value);
                                assertTrue("The value in " + catalog_col + " at row " + row + " is " + value.length() + ". Max is " + length, value.length() <= length);
                                break;
                            }
                            case TIMESTAMP: {
                                // TODO
                                break;
                            }
                            default:
                                assert (false) : "Unexpected type " + col_type + " for column " + catalog_col.getName() + " for row " + row;
                        } // SWITCH

                    } // FOR
                } // WHILE
                  // if (true ||
                  // tablename.equals(MarkovConstants.TABLENAME_TABLEB))
                  // LOG.info(table);
                table.resetRowPosition();

                // TABLEB and TABLEC Checks
                if (tablename.equals(MarkovConstants.TABLENAME_TABLEB) || tablename.equals(MarkovConstants.TABLENAME_TABLEC)) {
                    while (table.advanceRow()) {
                        long id = table.getLong(0);
                        long a_id = table.getLong(1);

                        // Make sure that the first and second columns are not
                        // equal
                        assertNotSame(tablename + ".ID and A_ID are the same value", id, a_id);

                        // And then make sure that the *_ID can be converted as
                        // a CompositeId
                        long decoded[] = MarkovLoader.decodeCompositeId(id);
                        assertEquals(a_id, decoded[0]);
                    } // WHILE
                }
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
        Field field_handle = MarkovConstants.class.getField(field_name);
        assertNotNull(field_handle);
        this.current_tablesize = Math.round((Long) field_handle.get(null) * SCALE_FACTOR);

        field_name = "BATCHSIZE_" + tablename;
        field_handle = MarkovConstants.class.getField(field_name);
        assertNotNull(field_handle);
        this.current_batchsize = (Long) field_handle.get(null);

        this.total_rows = 0l;
    }

    /**
     * testGenerateTABLEA
     */
    public void testGenerateTABLEA() throws Exception {
        this.setCurrentTable(MarkovConstants.TABLENAME_TABLEA);
        this.loader.generateTableData(MarkovConstants.TABLENAME_TABLEA);
    }

    /**
     * testGenerateTABLEB
     */
    public void testGenerateTABLEB() throws Exception {
        this.setCurrentTable(MarkovConstants.TABLENAME_TABLEB);
        this.loader.generateTableData(MarkovConstants.TABLENAME_TABLEB);
    }

    /**
     * testGenerateTABLEC
     */
    public void testGenerateTABLEC() throws Exception {
        this.setCurrentTable(MarkovConstants.TABLENAME_TABLEC);
        this.loader.generateTableData(MarkovConstants.TABLENAME_TABLEC);
    }

    /**
     * testGenerateTABLED
     */
    public void testGenerateTABLED() throws Exception {
        this.setCurrentTable(MarkovConstants.TABLENAME_TABLED);
        this.loader.generateTableData(MarkovConstants.TABLENAME_TABLED);
    }
}

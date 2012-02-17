package edu.brown.catalog;

import java.util.Collection;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.GetSubscriberData;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

public class TestCatalogUtil2 extends BaseTestCase {

    private static final int NUM_PARTITIONS = 10;

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITIONS);
    }

    /**
     * testGetReferencedColumns
     */
    public void testGetReferencedColumns() throws Exception {
        Procedure catalog_proc = this.getProcedure(GetSubscriberData.class);
        Statement catalog_stmt = CollectionUtil.first(catalog_proc.getStatements());
        assertNotNull(catalog_stmt);

        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        Column expected[] = { this.getColumn(catalog_tbl, "S_ID"), };

        Collection<Column> columns = CatalogUtil.getReferencedColumns(catalog_stmt);
        assertNotNull(columns);
        assertEquals(columns.toString(), expected.length, columns.size());
        for (Column c : expected) {
            assert (columns.contains(c)) : "Missing " + c.fullName();
        } // FOR
    }
}

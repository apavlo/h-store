package edu.brown.benchmark.users;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import edu.brown.api.BenchmarkComponent;
import edu.brown.api.Loader;
import edu.brown.catalog.CatalogUtil;

public class UsersLoader extends Loader{
    private static final Logger LOG = Logger.getLogger(UsersLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    private long articlesSize;
	private long usersSize;
	private long maxComments;
    public static void main(String[] args) {
        BenchmarkComponent.main(UsersLoader.class, args, true);
    }

    public UsersLoader(String[] args) {
        super(args);
        this.usersSize = Math.round(UsersConstants.USERS_SIZE * this.getScaleFactor());
    }

    @Override
    public void load() {
        if (d) LOG.debug("Starting ArticlesLoader");
        final Database catalog_db = this.getCatalogContext().database;

        if (d) LOG.debug("Start loading " + UsersConstants.TABLENAME_USERS);
        Table catalog_tbl = catalog_db.getTables().get(UsersConstants.TABLENAME_USERS);
        genUsers(catalog_tbl);
        if (d) LOG.debug("Finished loading " + UsersConstants.TABLENAME_USERS);

    }


    /**
     * Populate Users table per benchmark spec.
     */
    void genUsers(Table catalog_tbl) {
        final VoltTable table = CatalogUtil.getVoltTable(catalog_tbl);

/*-- u_firstname      User's first name
-- u_lastname       User's last name
-- u_password       User's password
-- u_email          User's email

 * 
 * */                
        long total = 0;
        for (long s_id = 0; s_id < this.usersSize; s_id++) {
                Object row[] = new Object[table.getColumnCount()];
                row[0] = s_id;
                row[1] = UsersUtil.astring(3, 3);
                row[2] = UsersUtil.astring(3, 3);
                row[3] = UsersUtil.astring(3, 3);
                row[4] = UsersUtil.astring(3, 3);
                row[5] = s_id;
                table.addRow(row);
                total++;
            if (table.getRowCount() >= UsersConstants.BATCH_SIZE) {
                if (d) LOG.debug(String.format("%s: %6d / %d",
                		UsersConstants.TABLENAME_USERS, total, usersSize));
                loadVoltTable(UsersConstants.TABLENAME_USERS, table);
                table.clearRowData();
            }
        } // WHILE
        if (table.getRowCount() > 0) {
            if (d) LOG.debug(String.format("%s: %6d / %d",
            		UsersConstants.TABLENAME_USERS, total, usersSize));
            loadVoltTable(UsersConstants.TABLENAME_USERS, table);
            table.clearRowData();
        }
    }

	 	 
	}


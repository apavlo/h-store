/**
 * 
 */
package edu.brown.catalog;

import java.io.File;
import java.io.IOException;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.utils.NotImplementedException;

import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

/**
 * @author pavlo
 */
public class CatalogExporter implements JSONSerializable {

    private final Catalog catalog;

    /**
     * Constructor
     * 
     * @param catalog
     */
    public CatalogExporter(Catalog catalog) {
        this.catalog = catalog;
    }

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#toJSON(org.json.JSONStringer)
     */
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        Database catalog_db = CatalogUtil.getDatabase(this.catalog);

        // Procedures
        stringer.key("PROCEDURES").object();
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc())
                continue;
            stringer.key(catalog_proc.getName()).object();
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                stringer.key(catalog_stmt.getName()).value(catalog_stmt.getSqltext());
            } // FOR
            stringer.endObject();
        } // FOR
        stringer.endObject();

        // Tables
        stringer.key("TABLES").object();
        for (Table catalog_tbl : catalog_db.getTables()) {
            stringer.key(catalog_tbl.getName()).object();

            stringer.key("COLUMNS").object();
            for (Column catalog_col : org.voltdb.utils.CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index")) {
                stringer.key(catalog_col.getName()).object();
                stringer.key("TYPE").value(VoltType.get(catalog_col.getType()).name());
                stringer.endObject();
            } // FOR
            stringer.endObject();

            stringer.endObject();
        } // FOR
        stringer.endObject();
    }

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#fromJSON(org.json.JSONObject,
     * org.voltdb.catalog.Database)
     */
    @Override
    public void fromJSON(JSONObject jsonObject, Database catalogDb) throws JSONException {
        throw new NotImplementedException("Cannot import JSON catalog");
    }

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#load(java.lang.String,
     * org.voltdb.catalog.Database)
     */
    @Override
    public void load(File inputPath, Database catalogDb) throws IOException {
        throw new NotImplementedException("Cannot import JSON catalog");
    }

    /*
     * (non-Javadoc)
     * @see edu.brown.utils.JSONSerializable#save(java.lang.String)
     */
    @Override
    public void save(File outputPath) throws IOException {
        JSONUtil.save(this, outputPath);
    }

    /*
     * (non-Javadoc)
     * @see org.json.JSONString#toJSONString()
     */
    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }

    /**
     * @param vargs
     * @throws Exception
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG, ArgumentsParser.PARAM_CATALOG_OUTPUT);

        Catalog catalog = args.catalog;
        File output = args.getFileParam(ArgumentsParser.PARAM_CATALOG_OUTPUT);
        new CatalogExporter(catalog).save(output);
    }

}

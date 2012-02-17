package edu.brown.catalog;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.CatalogProxy;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;

public abstract class PatchCatalog {
    /** java.util.logging logger. */
    private static final Logger LOG = Logger.getLogger(FixCatalog.class.getName());

    /**
     * @param args
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        assert (args.hasParam(ArgumentsParser.PARAM_CATALOG_OUTPUT));
        String output_path = args.getParam(ArgumentsParser.PARAM_CATALOG_OUTPUT);

        Procedure source_proc = args.catalog_db.getProcedures().get("neworderMultiSite");
        assert (source_proc != null);
        Procedure target_proc = args.catalog_db.getProcedures().get("neworder");
        assert (target_proc != null);

        Map<String, String> fix = new HashMap<String, String>();
        List<String> to_remove = new ArrayList<String>();

        for (Statement target_stmt : target_proc.getStatements()) {
            int idx = target_stmt.getName().indexOf("MultiSite");
            if (idx == -1)
                continue;
            String base_name = target_stmt.getName().substring(0, idx);
            Statement source_stmt = source_proc.getStatements().get(base_name);
            assert (target_stmt != null);

            // Copy fields
            for (String field : source_stmt.getFields()) {
                Object value = source_stmt.getField(field);
                if (!(value instanceof CatalogMap)) {
                    CatalogProxy.set(target_stmt, field, value);
                }
            } // FOR

            // Lines we need to remove
            String orig_catalog_id = null;
            String new_catalog_id = null;
            for (PlanFragment target_frag : target_stmt.getFragments()) {
                assert (orig_catalog_id == null);
                orig_catalog_id = target_frag.getName();
            } // FOR

            // Copy PlanFragments
            for (PlanFragment source_frag : source_stmt.getFragments()) {
                PlanFragment target_frag = target_stmt.getFragments().add(source_frag.getName());

                // Copy fields
                for (String field : source_frag.getFields()) {
                    Object value = source_frag.getField(field);
                    if (!(value instanceof CatalogMap)) {
                        CatalogProxy.set(target_frag, field, value);
                    }
                } // FOR

                if (new_catalog_id == null) {
                    new_catalog_id = target_frag.getName();
                    to_remove.add(target_frag.getName());
                    StringBuilder buffer = new StringBuilder();
                    CatalogProxy.writeCommands(target_frag, buffer);
                    fix.put(orig_catalog_id, buffer.toString().replaceAll(source_proc.getName(), target_proc.getName()));
                }
            } // FOR
        } // FOR

        // LOG.info("LINES TO REMOVE: " + to_remove);
        // LOG.info("IDS TO FIX: " + fix);
        String serialized = args.catalog.serialize();
        StringBuilder buffer = new StringBuilder();
        for (String line : serialized.split("\n")) {
            // Remove
            boolean include = true;
            for (String remove : to_remove) {
                if (line.indexOf(remove) != -1 && line.indexOf(source_proc.getName()) == -1) {
                    include = false;
                    break;
                }
            } // FOR
            if (!include)
                continue;

            // Fix
            String fixed = null;
            for (String orig_catalog_id : fix.keySet()) {
                if (line.indexOf(orig_catalog_id) != -1) {
                    line = fix.get(orig_catalog_id);
                    fixed = orig_catalog_id;
                    break;
                }
            } // FOR
            buffer.append(line);

            if (fixed != null) {
                fix.remove(fixed);
                to_remove.add(fixed);
            } else {
                buffer.append("\n");
            }
        } // FOR

        FileUtil.writeStringToFile(new File(output_path), buffer.toString());
        LOG.info("Wrote updated catalog specification to '" + output_path + "'");

        return;
    }

}

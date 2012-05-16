package edu.brown.hstore;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

/**
 * Invoke a stored procedure programmatically
 * @author pavlo
 * @author evanj
 */
public class VoltProcedureInvoker {
    private static final Logger LOG = Logger.getLogger(VoltProcedureInvoker.class);
    
    /**
     * Invoke a stored procedure using the given client handle
     * The string parameters will be automatically converted to proper type 
     * Throws an exception if there was an error
     * @param catalog
     * @param client
     * @param procName
     * @param params
     * @return
     * @throws
     */
    public static ClientResponse invoke(Catalog catalog,
                                         Client client,
                                         String procName,
                                         String...params) throws Exception {
        Database catalog_db = CatalogUtil.getDatabase(catalog);
        
        // Get Procedure catalog handle
        if (procName == null || procName.isEmpty()) {
            LOG.error("Missing procedure name");
            return (null);
        }
        Procedure catalog_proc = catalog_db.getProcedures().getIgnoreCase(procName);
        if (catalog_proc == null) {
            throw new Exception("Invalid procedure name '" + procName + "'");
        }
        
        int expectedParams = catalog_proc.getParameters().size();
        // Hack for @AdHoc
        if (catalog_proc.getName().equalsIgnoreCase("@AdHoc")) {
            expectedParams = 1;
        }
        
        // Procedure Parameters
        if (params.length != expectedParams) {
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            for (ProcParameter catalog_param : CatalogUtil.getSortedCatalogItems(catalog_proc.getParameters(), "index")) {
                VoltType vtype = VoltType.get(catalog_param.getType());
                String key = String.format("[%02d]", catalog_param.getIndex()); 
                String val = vtype.name();
                
                if (vtype == VoltType.TIMESTAMP) {
                    val += " (FORMATS: ";
                    String add = "";
                    int spacer = val.length();
                    for (String f : VoltTypeUtil.DATE_FORMAT_PATTERNS) {
                        val += add + f;
                        add = "\n" + StringUtil.repeat(" ", spacer);
                    }
                    val += ")";
                }
                
                m.put(key, val);
            }
            throw new Exception(String.format("Incorrect number of parameters for %s. Expected %d, but was only given %d\n" +
                                    "Expected Input Parameters:\n%s",
                                    catalog_proc, catalog_proc.getParameters().size(), params.length,
                                    StringUtil.prefix(StringUtil.formatMaps(m), "  ")).trim());
        }
        
        // Convert parameters to the proper type
        Object parameters[] = new Object[expectedParams];
        for (int i = 0; i < expectedParams; i++) {
            ProcParameter catalog_param = catalog_proc.getParameters().get(i);
            assert(catalog_param != null) : String.format("Null %s parameter at %d", catalog_proc.getName(), i); 
            VoltType vt = VoltType.get(catalog_param.getType());
            
            try {
                // Split into array
                if (catalog_param.getIsarray()) {
                    List<String> arr = (List<String>)CollectionUtil.addAll(new ArrayList<String>(),
                                                                           params[i].split(","));
                    Object inner[] = new Object[arr.size()];
                    for (int ii = 0, cnt = arr.size(); ii < cnt; ii++) {
                        inner[ii] = VoltTypeUtil.getObjectFromString(vt, arr.get(ii));
                    } // FOR
                    parameters[i] = inner;
                // Scalar Paramter
                } else {
                    parameters[i] = VoltTypeUtil.getObjectFromString(vt, params[i]);
                }
            } catch (ParseException ex) {
                LOG.error("Invalid parameter #" + i + ": " + params[i], ex);
                return (null);
            }
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("%s: %s [%s / %s]", catalog_param.fullName(),
                                                            (catalog_param.getIsarray() ? Arrays.toString((Object[])parameters[i]) : parameters[i]),
                                                            vt, parameters[i].getClass()));
        } // FOR
        
        LOG.info(String.format("Invoking %s [params=%s]",
                               catalog_proc.getName(), Arrays.toString(parameters)));
        ClientResponse cresponse = client.callProcedure(catalog_proc.getName(), parameters);
        
        return (cresponse);
    }
    
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG);
        
        String procName = args.getOptParam(0);
        String parameters[] = new String[args.getOptParamCount()-1];
        for (int i = 0; i < parameters.length; i++) {
            parameters[i] = args.getOptParam(i+1);
        } // FOR
        
        Client client = ClientFactory.createClient(128, null, false, null);
        Cluster catalog_clus = args.catalog_db.getParent(); 
        Site catalog_site = CollectionUtil.first(catalog_clus.getSites());
        assert(catalog_site != null);
        Host catalog_host = catalog_site.getHost();
        assert(catalog_host != null);
        Integer port = CollectionUtil.random(CatalogUtil.getExecutionSitePorts(catalog_site));
        assert(port != null);
        client.createConnection(null, catalog_host.getIpaddr(), port, "user", "password");
        LOG.info(String.format("Connected to H-Store cluster at %s:%d", catalog_host.getIpaddr(), port));
        
        ClientResponse cresponse = VoltProcedureInvoker.invoke(args.catalog,
                                                               client,
                                                               procName,
                                                               parameters);
        if (cresponse == null) System.exit(-1);
        
//        m.put("Status", cresponse.getStatus());
//        m.put("Length", String.format("%d [bytes=%d]", cresponse.getResults().length, cresponse.getResultsSize()));
//        m.put("Results", StringUtil.join("\n", ));
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        for (int i = 0; i < cresponse.getResults().length; i++) {
            VoltTable vt = cresponse.getResults()[i];
            m.put(String.format("  [%02d]", i), vt);
        } // FOR
        
        LOG.info(StringUtil.repeat("-", 50));
        LOG.info(String.format("%s Txn #%d - Status %s\n%s",
                               procName,
                               cresponse.getTransactionId(),
                               cresponse.getStatus(),
                               cresponse.toString()));
    }
}

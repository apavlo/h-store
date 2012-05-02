package edu.brown.hstore;

import java.util.Arrays;
import java.util.Map;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Cluster;
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

public class VoltProcedureInvoker {
    private static final Logger LOG = Logger.getLogger(VoltProcedureInvoker.class);
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG);
        
        String procName = args.getOptParam(0);
        assert(procName != null && procName.isEmpty() == false) : "Invalid procedure name '" + procName + "'";
        Procedure catalog_proc = args.catalog_db.getProcedures().getIgnoreCase(procName);
        assert(catalog_proc != null) : "Invalid procedure name '" + procName + "'";
        
        if (args.getOptParamCount()-1 != catalog_proc.getParameters().size()) {
            
            Map<String, Object> m = new ListOrderedMap<String, Object>();
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
            LOG.error(String.format("Incorrect number of parameters for %s. Expected %d, but was only given %d\n" +
                                    "Expected Input Parameters:\n%s",
                                    catalog_proc, catalog_proc.getParameters().size(), args.getOptParamCount()-1,
                                    StringUtil.prefix(StringUtil.formatMaps(m), "  ")).trim());
            System.exit(1);
        }
            
        Object parameters[] = new Object[catalog_proc.getParameters().size()];
        for (int i = 0; i < parameters.length; i++) {
            ProcParameter catalog_param = catalog_proc.getParameters().get(i);
            assert(catalog_param != null) : String.format("Null %s parameter at %d", catalog_proc.getName(), i); 
            VoltType vt = VoltType.get(catalog_param.getType());
            parameters[i] = args.getOptParam(i+1, vt);
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("%s: %s [%s / %s]", catalog_param.fullName(),
                                                            parameters[i],
                                                            vt, parameters[i].getClass()));
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
        

        
        LOG.info(String.format("Invoking %s at %s:%d [params=%s]",
                               catalog_proc.getName(), catalog_host.getIpaddr(), port, Arrays.toString(parameters)));
        long start = System.nanoTime();
        ClientResponse cresponse = client.callProcedure(catalog_proc.getName(), parameters);
        long stop = System.nanoTime();
        
//        m.put("Status", cresponse.getStatus());
//        m.put("Length", String.format("%d [bytes=%d]", cresponse.getResults().length, cresponse.getResultsSize()));
//        m.put("Results", StringUtil.join("\n", ));
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (int i = 0; i < cresponse.getResults().length; i++) {
            VoltTable vt = cresponse.getResults()[i];
            m.put(String.format("  [%02d]", i), vt);
        } // FOR
        
        LOG.info(StringUtil.repeat("-", 50));
        
        LOG.info(String.format("%s Txn #%d - Status %s - Time %.2f ms\n%s",
                               catalog_proc.getName(),
                               cresponse.getTransactionId(),
                               cresponse.getStatus(),
                               (stop - start) / 1000000d,
                               cresponse.toString()));
    }
}

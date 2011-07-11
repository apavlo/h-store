package edu.mit;

import java.util.Map;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.voltdb.VoltType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;

import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

public class VoltProcedureInvoker {

    //private final static Object[] EMPTY_ARRAY = {};
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG);
        
        Client client = ClientFactory.createClient(128, null, false, null);
        
        Cluster catalog_clus = args.catalog_db.getParent(); 
        Site catalog_site = CollectionUtil.getFirst(catalog_clus.getSites());
        assert(catalog_site != null);
        Host catalog_host = catalog_site.getHost();
        assert(catalog_host != null);
        client.createConnection(catalog_host.getIpaddr(), catalog_site.getProc_port(), "user", "password");
        
        String procName = args.getOptParam(0);
        assert(procName != null && procName.isEmpty() == false) : "Invalid procedure name '" + procName + "'";
        Procedure catalog_proc = args.catalog_db.getProcedures().getIgnoreCase(procName);
        assert(catalog_proc != null) : "Invalid procedure name '" + procName + "'";
        
        Object parameters[] = new Object[args.getOptParamCount() - 1];
        for (int i = 0; i < parameters.length; i++) {
            ProcParameter catalog_param = catalog_proc.getParameters().get(i);
            assert(catalog_param != null) : String.format("Null %s parameter at %d", catalog_proc.getName(), i); 
            VoltType vt = VoltType.get(catalog_param.getType());
            parameters[i] = args.getOptParam(i+1, vt);
        }
        
        ClientResponse cresponse = client.callProcedure(catalog_proc.getName(), parameters);
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        m.put("Status", cresponse.getStatusName());
        m.put("Length", String.format("%d [bytes=%d]", cresponse.getResults().length, cresponse.getResultsSize()));
        m.put("Results", StringUtil.join("\n", cresponse.getResults()));
        System.out.println(StringUtil.formatMaps(m));

    }
}

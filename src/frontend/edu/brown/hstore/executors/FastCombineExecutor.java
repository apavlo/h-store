package edu.brown.hstore.executors;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.DependencySet;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.catalog.PlanFragment;

import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.dtxn.LocalTransaction;

public class FastCombineExecutor extends FastExecutor {

	public FastCombineExecutor(PartitionExecutor executor) {
		super(executor);
		// TODO Auto-generated constructor stub
	}

	@Override
	public VoltTable execute(LocalTransaction ts, PlanFragment catalog_frag,
			VoltTable[] input) {
		// TODO Auto-generated method stub
		return null;
	}
	
	 /**
     * Execute a Java-only operation to generate the output of a PlanFragment for 
     * the given transaction without needing to go down in to ExecutionEngine
     * @param ts 
     * @param catalog_frag
     * @param input
     * @return
     */
	public DependencySet executeFastCombine(int id,Map<Integer, List<VoltTable>> tmp_dependencies){
		Set<Integer> keys=tmp_dependencies.keySet();
		Object[] Okey=keys.toArray();
		VoltTable record=null;
		Object key=Okey[0];
 		List<VoltTable> tmp=tmp_dependencies.get(key);
 	    record=tmp.get(0).clone(0);
 	    
 	    for(int i=0;i<tmp.size();i++){
   		 VoltTable t=tmp.get(i);
   		
   	     while(t.advanceRow()){
   	    	VoltTableRow r=t.getRow();
   	    	record.add(r);
   	    	
   	     }//while         	
   		
   	    }//for
 	   int[] depid;
       depid=new int[1];
       depid[0]=id;
       VoltTable[] vt;
       vt=new VoltTable[1];
       vt[0]=record;
       DependencySet result=new DependencySet(depid,vt);
       assert(result != null);
   	   return result;
		
	}

}

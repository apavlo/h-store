package edu.brown.hstore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import javax.swing.text.html.HTMLDocument.Iterator;

import edu.brown.hstore.txns.AbstractTransaction;

public class WorkflowScheduler {

    private final HStoreSite hstore_site;

    // workflowid - sp name - status (started, ended) - true
    // used to record the executed txn status
    private Map< Integer, Map<String,Map<String,Boolean>> > m_status ;
    
    // 
    private final BlockingDeque<AbstractTransaction> backupQueue;
    
    private Integer m_currentWorkflowID = -2;
    private String firstSP = null;
    
    public void debug()
    {
        System.out.println("--- Begining debug()... ");
        for (Map.Entry<Integer, Map<String,Map<String,Boolean>>> entry : m_status.entrySet()) {
            Integer workflowid = entry.getKey();
            Map<String,Map<String,Boolean>> txnmap = entry.getValue();
            
            for (Map.Entry<String, Map<String,Boolean>> txnEntry : txnmap.entrySet())
            {
                String txn = txnEntry.getKey();
                Map<String,Boolean> statusmap = txnEntry.getValue();
                
                for (Map.Entry<String, Boolean> statusEntry: statusmap.entrySet())
                {
                    String status = statusEntry.getKey();
                    Boolean value = statusEntry.getValue();
                    
                    System.out.println("item: " + workflowid + " - " + txn + " - " + status + " - " + value );
                }
            }
        }
        System.out.println("--- Ending debug()... ");
    }
    
    public WorkflowScheduler(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        
        m_status = new ConcurrentHashMap<Integer, Map<String,Map<String,Boolean>>>();
        
        this.backupQueue = new LinkedBlockingDeque<AbstractTransaction>(); 
    }
    
    public boolean isBackupQueueEmpty()
    {
        return backupQueue.isEmpty();
    }
    
    public void addBackupQueue(AbstractTransaction txn)
    {
        backupQueue.addFirst(txn);
    }
    
    public void backupQueue(BlockingDeque<AbstractTransaction> initQueue)
    {
        java.util.Iterator<AbstractTransaction> it= backupQueue.iterator();
        while(it.hasNext()==true){
            initQueue.addFirst(it.next());
            it.remove();
        }

    }
    
//    1 - create workflow (workflowid)
    public void createWorkflow(Integer workflowid)
    {
        Map<String,Map<String,Boolean>> txnStatus = new ConcurrentHashMap<String,Map<String,Boolean>>();
        m_status.put(workflowid, txnStatus);
    }
    
    //public void getWorkflow(integer workflowid)
    
//    2 - delete workflow (workflowid)
    public void deleteWorkflow(Integer workflowid)
    {
        m_status.remove(workflowid);
    }
    
    public Integer getCurrentWorkflowID()
    {
        return this.m_currentWorkflowID;
    }
    
//    3 - add sp status (workflowid, spid, status)
    public void addSPStartStatus(Integer workflowid, String txn)
    {
        this.addSPStatus(workflowid, txn, "started");
    }
    
    public void addSPEndStatus(Integer workflowid, String txn)
    {
        this.addSPStatus(workflowid, txn, "ended");
    }
    
    private void addSPStatus(Integer workflowid, String txn, String status)
    {
        System.out.println("addSPStatus - " + workflowid + " - " + txn + " - " + status);
        Map<String,Map<String,Boolean>> txnStatus = m_status.get(workflowid);
        
        if(txnStatus==null)
        {
            txnStatus = new ConcurrentHashMap<String,Map<String,Boolean>>();
            m_currentWorkflowID = workflowid;
            firstSP = txn;
            m_status.put(workflowid, txnStatus);
        }
        
        Map<String,Boolean> ss = txnStatus.get(txn);
        if(ss==null)
        {
            ss = new ConcurrentHashMap<String, Boolean>();
            txnStatus.put(txn, ss);
        }
        
        ss.put(status, true);
        
    }
//    4 - isFinished(worflowid)
    public boolean isWorkflowFinished()
    {
        System.out.println("isWorkflowFinished - firstSP - " + firstSP);
        
        if(firstSP==null)
            return true;
        
        boolean result = isSPFinished(firstSP);

        this.debug();
        System.out.println("isWorkflowFinished - result - " + result);

        return result;
        
    }
//    5 - isFinished(worflowid, spid)  [private method]
    public boolean isSPFinished( String txn )
    {
//        if(m_currentWorkflowID==-1)
//            return true;
        
//        (1) <workflowid, <spid,'started'>> is in S ;
          if (isSPStarted(txn)==false)
              return false;
          
//        (2) <workflowid, <spid,'ended'>> is in S ;
          if(isSPEnded(txn)==false)
              return false;
          
//        (3) children condition should satisfy:  (we should get children information from T)
//             (3.1) no children at all;
          List<String> children = getExecutedChildren(txn);
          if((children==null) || (children.isEmpty()==true))
              return true;
//             or,
//             (3.2) for each child with child_spid of sp, satisfy
          for(int i=0;i<children.size();i++)
          {
              String child_txn = children.get(i);
              if(/*(hasNoRecord(child_txn)==false)&&*/(isSPFinished(child_txn)==false))
                  return false;
//                 (3.2.1) no <workflowid, <child_spid,'started'>> and <workflowid, <child_spid,'ended'>> in S;
//                            that means logically this child is not triggered yet
//                 or,
//                 (3.2.2) this child sp (child_spid) is finished; (Recursion) 
          }
        return true;
    }
    
    private List<String> getExecutedChildren(String txn)
    {
        List<String> children = new ArrayList<String>();
        
        // 1 - get child procedures of the txn using m_workflowTopology from HStoreSite
        String currentProcedureName =  txn;
        Map<String, List<String>> workflowTopology = hstore_site.getWorflowTopology();
        List<String> frontendtriggers = workflowTopology.get(currentProcedureName);
        
        if(frontendtriggers==null)
            return null;
        
        // 2 - for each child procedure, find the txn in the m_status with the current workflow id
        // 3 - put them in the children
        for (int i=0; i< frontendtriggers.size(); i++)
        {
            String child = frontendtriggers.get(i);
            if(hasNoRecord(child)==false)
                children.add(child);
        }
        
        return children;
    }
    
    private boolean isSPStarted(String txn)
    {
//        if (m_currentWorkflowID==-1)
//            return false;
        Map<String,Map<String,Boolean>> txnStatus = m_status.get(this.m_currentWorkflowID);
        Map<String,Boolean> ss = txnStatus.get(txn);
        if(ss==null)
            return false;
        Boolean status = ss.get("started");
        if(status == null)
            return false;
        return true;
    }
    
    private boolean isSPEnded(String txn)
    {
//        if (m_currentWorkflowID==-1)
//            return false;
        Map<String,Map<String,Boolean>> txnStatus = m_status.get(this.m_currentWorkflowID);
        Map<String,Boolean> ss = txnStatus.get(txn);
        if(ss==null)
            return false;
        Boolean status = ss.get("ended");
        if(status == null)
            return false;
        return true;
    }
    
    private boolean hasNoRecord(String txn)
    {
        Map<String,Map<String,Boolean>> txnStatus = m_status.get(this.m_currentWorkflowID);
        Map<String,Boolean> ss = txnStatus.get(txn);
        if(ss==null)
            return true;
        return false;
    }

    public AbstractTransaction getScheduledNextTxn(AbstractTransaction nextTxn, BlockingDeque<AbstractTransaction> initQueue) {
        
        AbstractTransaction scheduledNextTxn = null;
        
        Integer workflowid = nextTxn.getBatchId();
        String spname = nextTxn.getProcedure().getName();
       
        Integer currentWkfID = this.getCurrentWorkflowID();
        boolean isWkfFinished = this.isWorkflowFinished();
        boolean isSameWkfID = workflowid.equals(currentWkfID);
        
        System.out.println(spname + "-workflowid-" +workflowid+"-currentworkflowid-" + currentWkfID +"-isWkfFinished:"+isWkfFinished);
        // if current workflow is finished and we have new workflow id
        // we will delete the finished workflow execution status from records
        // then we should check if the backup queue has txn items there,
        // if yes, we should put them back to initQueue
        // if no, just continue
        if((isWkfFinished==true)/*&&(currentWkfID != workflowid)*/)
        {
            System.out.println("getScheduledNextTxn - 1");
            this.deleteWorkflow(currentWkfID);
            if(this.isBackupQueueEmpty()==false)
            {
                this.addBackupQueue(nextTxn);
                this.backupQueue(initQueue);
                scheduledNextTxn = null;
            }
            else
                scheduledNextTxn = nextTxn;
        }
        // if we have new workflow id but current workflow is not finished yet
        // we should put the txn to backupQueue to let it run later after current workflow finish
        // and continue to next txn until get the correct one for current workflow
        if((isWkfFinished==false)&&(isSameWkfID==false))
        {
            System.out.println("getScheduledNextTxn - 2");
            this.addBackupQueue(nextTxn);
            scheduledNextTxn = null;
        }
        // if current workflow is not finished and current txn has the same workflow id
        // since this txn beongs to current workflow, 
        // we just need to run it (nothing else need to be done)
        if((isWkfFinished==false)&&(isSameWkfID==true))
        {
            System.out.println("getScheduledNextTxn - 3");
            scheduledNextTxn = nextTxn;
        }

        if(scheduledNextTxn != null)
        {
            System.out.println("getScheduledNextTxn - 4");
            this.addSPStartStatus( workflowid, spname );
        }
        
        return scheduledNextTxn;
    }

}

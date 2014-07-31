package edu.brown.hstore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import javax.swing.text.html.HTMLDocument.Iterator;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Procedure;

import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.DumbTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class WorkflowScheduler {

    private static final Logger LOG = Logger.getLogger(WorkflowScheduler.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }


    private final HStoreSite hstore_site;

    // workflowid - sp name - status (started, ended) - true
    // used to record the executed txn status
    private Map< Long, Map<String,Map<String,Boolean>> > m_status ;
    
    // 
    private final BlockingDeque<AbstractTransaction> backupQueue;
    
    private Long m_currentWorkflowID = 0l;
    private String firstSP = null;
    
    private void debug()
    {
        System.out.println("--- Begining debug()... ");
        for (Map.Entry<Long, Map<String,Map<String,Boolean>>> entry : m_status.entrySet()) {
            Long workflowid = entry.getKey();
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
        
        m_status = new ConcurrentHashMap<Long, Map<String,Map<String,Boolean>>>();
        
        this.backupQueue = new LinkedBlockingDeque<AbstractTransaction>(); 
    }
    
    private boolean isBackupQueueEmpty()
    {
        return backupQueue.isEmpty();
    }
    
    private void addBackupQueue(AbstractTransaction txn)
    {
        backupQueue.addFirst(txn);
        //backupQueue.addLast(txn);
    }
    
    private void backupQueue(BlockingDeque<AbstractTransaction> initQueue)
    {
        java.util.Iterator<AbstractTransaction> it= backupQueue.iterator();
        while(it.hasNext()==true){
            initQueue.addFirst(it.next());
            it.remove();
        }

    }
    
    private void debugQueue(BlockingDeque<AbstractTransaction> initQueue)
    {
        LOG.debug("------------------ debugQueue start ---------------------");
        java.util.Iterator<AbstractTransaction> it= initQueue.iterator();
        while(it.hasNext()==true){
            AbstractTransaction txn =  it.next();
            if (DumbTransaction.class.isInstance(txn) == true)
            {
                LOG.debug("DumbTransaction");
            }
            else
            {
                Long workflowid = txn.getBatchId();
                String spname = txn.getProcedure().getName();
                LOG.debug(spname + "- workflowid-" + workflowid);
            }
        }   
        LOG.debug("------------------ debugQueue end ---------------------");
    }
    
//    1 - create workflow (workflowid)
    private void createWorkflow(Long workflowid)
    {
        Map<String,Map<String,Boolean>> txnStatus = new ConcurrentHashMap<String,Map<String,Boolean>>();
        m_status.put(workflowid, txnStatus);
    }
    
    //public void getWorkflow(integer workflowid)
    
//    2 - delete workflow (workflowid)
    private void deleteWorkflow(Long workflowid)
    {
        m_status.remove(workflowid);
        firstSP = null;
    }
    
    public Long getCurrentWorkflowID()
    {
        return this.m_currentWorkflowID;
    }
    
//    3 - add sp status (workflowid, spid, status)
    public void addSPStartStatus(Long workflowid, String txn)
    {
        this.addSPStatus(workflowid, txn, "started");
        LOG.debug("WORKFLOW started: " + workflowid + " " + txn);
    }
    
    public void addSPEndStatus(Long workflowid, String txn)
    {
        if(workflowid != -1l)
        {
            this.addSPStatus(workflowid, txn, "ended");
            LOG.debug("WORKFLOW ended: " + workflowid + " " + txn);
        }
        LOG.debug("WORKFLOW should end, but workflowid was -1: " + workflowid + " " + txn);
    }
    
    private synchronized void addSPStatus(Long workflowid, String txn, String status)
    {
        if (debug.val)
            LOG.debug("addSPStatus - " + workflowid + " - " + txn + " - " + status);
        //System.out.println("addSPStatus - " + workflowid + " - " + txn + " - " + status);
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
    private boolean isWorkflowFinished()
    {
        //System.out.println("isWorkflowFinished - firstSP - " + firstSP);
        
        if(firstSP==null)
            return true;
        
        boolean result = isSPFinished(firstSP);

        //this.debug();
        //System.out.println("isWorkflowFinished - result - " + result);

        return result;
        
    }
//    5 - isFinished(worflowid, spid)  [private method]
    private boolean isSPFinished( String txn )
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
          //System.out.println("workflow - " + this.m_currentWorkflowID + "SP - " + txn + " has executing frontend triggers...");
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

    public synchronized AbstractTransaction getScheduledNextTxn(AbstractTransaction nextTxn, BlockingDeque<AbstractTransaction> initQueue) {

        Long currentWkfID = this.getCurrentWorkflowID();


        if (DumbTransaction.class.isInstance(nextTxn) == true) {
//            if (debug.val)
//                LOG.debug("getScheduledNextTxn - DumbTransaction");
            if (this.isWorkflowFinished() != true) {
                initQueue.addLast(nextTxn);
            }
            else
            {
//                if (debug.val)
//                    LOG.debug("dump back queue ... ");
                this.deleteWorkflow(currentWkfID);
                if(this.isBackupQueueEmpty()==false)
                {
                    //this.addBackupQueue(nextTxn);
                    this.backupQueue(initQueue);
                }
            }
            return null;
        }
        
        // if not dumb transaction, means that we do not need to wait
        boolean isWkfFinished = this.isWorkflowFinished();
        AbstractTransaction scheduledNextTxn = null;
        
        Long workflowid = nextTxn.getBatchId();
        String spname = nextTxn.getProcedure().getName();
       
        // if the txn is system procedure related
        if(workflowid==-1l)
        {
            //System.out.println(spname + "- direct execute workflowid-" +workflowid);
            return nextTxn;
        }
        
        boolean isSameWkfID = workflowid.equals(currentWkfID);
        
//        if (debug.val)
//        {
//            LOG.debug(spname + "-workflowid-" +workflowid+"-currentworkflowid-" + currentWkfID +"-isWkfFinished:"+isWkfFinished);
//            debugQueue(initQueue);
//            debugQueue(this.backupQueue);
//        }
        // if current workflow is finished and we have new workflow id
        // we will delete the finished workflow execution status from records
        // then we should check if the backup queue has txn items there,
        // if yes, we should put them back to initQueue
        // if no, just continue
        if((isWkfFinished==true)/*&&(currentWkfID != workflowid)*/)
        {
//            if (debug.val)
//                LOG.debug("getScheduledNextTxn - 1");
            this.deleteWorkflow(currentWkfID);
            if(this.isBackupQueueEmpty()==false)
            {
//                if (debug.val)
//                    LOG.debug("put " + spname + "-workflowid-" +workflowid + " to backupqueue...");
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
//            if (debug.val)
//                LOG.debug("put " + spname + "-workflowid-" +workflowid + " to backupqueue...");
            this.addBackupQueue(nextTxn);
            initQueue.addLast(new DumbTransaction(this.hstore_site));
            //initQueue.addLast(nextTxn);
            scheduledNextTxn = null;
            //System.out.println("getScheduledNextTxn - 2 - end");
        }
        // if current workflow is not finished and current txn has the same workflow id
        // since this txn beongs to current workflow, 
        // we just need to run it (nothing else need to be done)
        if((isWkfFinished==false)&&(isSameWkfID==true))
        {
            //System.out.println("getScheduledNextTxn - 3");
            scheduledNextTxn = nextTxn;
        }

        if(scheduledNextTxn != null)
        {
//            if (debug.val)
//                LOG.debug("getScheduledNextTxn - 4");
            
            Procedure proc = scheduledNextTxn.getProcedure();
            // if it is not a frontend trigger, we record start here
            if(proc.getBedefault()!=true)
                this.addSPStartStatus( workflowid, spname );
        }
        
        return scheduledNextTxn;
    }

}

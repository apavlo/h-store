package edu.brown.hstore.reconfiguration;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.brown.hashing.PlannedHasher;
import edu.brown.hashing.ReconfigurationPlan;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.reconfiguration.ReconfigurationConstants.ReconfigurationProtocols;
import edu.brown.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class ReconfigurationCoordinator implements Shutdownable {
  public static final Logger LOG = Logger.getLogger(ReconfigurationCoordinator.class);
  private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
  private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
  
  //Cached list of local executors
  private List<PartitionExecutor> local_executors;
  static {
      LoggerUtil.setupLogging();
      LoggerUtil.attachObserver(LOG, debug, trace);
  }
  
  public enum ReconfigurationStates {
    NORMAL,
    INIT,
    START,
    DONE, 
    INIT_LEADER,
    START_LEADER,
    DONE_LEADER
  }
  public ReconfigurationStates reconfiguration_state;
  private HStoreSite hstore_site;
  
  public ReconfigurationCoordinator(HStoreSite hstore_site) {
    // TODO Auto-generated constructor stub
    reconfiguration_state = ReconfigurationStates.NORMAL;
    this.hstore_site = hstore_site;
    local_executors = new ArrayList<>();
    for (int p_id : hstore_site.getLocalPartitionIds().values()){
       local_executors.add(hstore_site.getPartitionExecutor(p_id));
    }
    
  }

  @Override
  public void prepareShutdown(boolean error) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void shutdown() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean isShuttingDown() {
    // TODO Auto-generated method stub
    return false;
  }

  public void initReconfiguration(int coordinator_site_id,
      String partition_plan, ReconfigurationProtocols protocol) {
    // TODO Auto-generated method stub
    if(this.hstore_site.getSiteId()==coordinator_site_id){
      if(debug.val) LOG.debug("Setting site as reconfig leader");
      reconfiguration_state = ReconfigurationStates.INIT_LEADER;      
    }
    else{
        reconfiguration_state = ReconfigurationStates.INIT;
    }
    PlannedHasher hasher = (PlannedHasher)hstore_site.getHasher();
    ReconfigurationPlan reconfig_plan;
    try {
        reconfig_plan = hasher.changePartitionPhase(partition_plan);
        if(reconfig_plan != null){
            for(PartitionExecutor executor : local_executors){
                executor.setReconfigurationPlan(reconfig_plan);
                executor.setReconfiguration_state(ReconfigurationStates.INIT);
            }
        }
    } catch (Exception e) {
        LOG.error(e);
        throw new RuntimeException(e);
    }

    
    
  }

}

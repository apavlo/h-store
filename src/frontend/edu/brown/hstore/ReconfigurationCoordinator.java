package edu.brown.hstore;

import org.apache.log4j.Logger;

import edu.brown.hstore.reconfiguration.ReconfigurationConstants.ReconfigurationProtocols;
import edu.brown.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class ReconfigurationCoordinator implements Shutdownable {
  public static final Logger LOG = Logger.getLogger(ReconfigurationCoordinator.class);
  private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
  private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
  
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
  public ReconfigurationStates reconfigurationState;
  private HStoreSite hstore_site;
  
  public ReconfigurationCoordinator(HStoreSite hstore_site) {
    // TODO Auto-generated constructor stub
    reconfigurationState = ReconfigurationStates.NORMAL;
    this.hstore_site = hstore_site;
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
      //LEFTOFF
    }
    
  }

}

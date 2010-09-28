package edu.brown.designer.indexselectors;

import org.apache.log4j.Logger;

import edu.brown.designer.*;
import edu.brown.designer.partitioners.PartitionPlan;

/**
 * 
 * @author pavlo
 *
 */
public abstract class AbstractIndexSelector {
    protected static final Logger LOG = Logger.getLogger(AbstractIndexSelector.class.getName());
    
    protected final Designer designer;
    protected final DesignerInfo info;
    
    public AbstractIndexSelector(Designer designer, DesignerInfo info) {
        this.designer = designer;
        this.info = info;
    }
    
    /**
     * 
     * @param <T>
     * @throws Exception
     */
    public abstract IndexPlan generate(PartitionPlan plan) throws Exception;
}

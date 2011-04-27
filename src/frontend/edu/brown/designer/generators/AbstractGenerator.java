package edu.brown.designer.generators;

import edu.brown.designer.DesignerInfo;
import edu.brown.designer.Edge;
import edu.brown.designer.Vertex;
import edu.brown.graphs.*;

public abstract class AbstractGenerator<T extends IGraph<Vertex, Edge>> {
    protected final DesignerInfo info;
    protected boolean debug;
    
    public AbstractGenerator(DesignerInfo info) {
        assert(info != null);
        this.info = info;
    }
    
    public DesignerInfo getInfo() {
        return this.info;
    }
    
    public void setDebug(boolean debug) {
        this.debug = debug;
    }
    
    /**
     * 
     * @param <T>
     * @param agraph
     * @throws Exception
     */
    public abstract void generate(T graph) throws Exception;
    
}

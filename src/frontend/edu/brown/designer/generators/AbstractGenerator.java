package edu.brown.designer.generators;

import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.DesignerVertex;
import edu.brown.graphs.IGraph;

public abstract class AbstractGenerator<T extends IGraph<DesignerVertex, DesignerEdge>> {
    protected final DesignerInfo info;
    protected boolean debug;

    public AbstractGenerator(DesignerInfo info) {
        assert (info != null);
        this.info = info;
    }

    public DesignerInfo getInfo() {
        return this.info;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    /**
     * @param <T>
     * @param agraph
     * @throws Exception
     */
    public abstract void generate(T graph) throws Exception;

}

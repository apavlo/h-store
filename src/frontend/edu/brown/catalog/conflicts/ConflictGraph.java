package edu.brown.catalog.conflicts;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.types.ConflictType;

import edu.brown.graphs.AbstractDirectedGraph;
import edu.brown.graphs.AbstractEdge;
import edu.brown.graphs.AbstractVertex;
import edu.uci.ics.jung.graph.util.EdgeType;

/**
 * A ConflictGraph is a representation of the different type of
 * consistency conflicts between stored procedures
 * @author pavlo
 */
public class ConflictGraph extends AbstractDirectedGraph<ConflictGraph.ConflictVertex, ConflictGraph.ConflictEdge> {
    private static final Logger LOG = Logger.getLogger(ConflictGraph.class);
    private static final long serialVersionUID = 1L;
    
    public static class ConflictVertex extends AbstractVertex {
        public ConflictVertex(Procedure catalog_proc) {
            super(catalog_proc);
        }
    }
    
    public static class ConflictEdge extends AbstractEdge {
        private final ConflictType type;
        public ConflictEdge(ConflictGraph graph, ConflictType type) {
            super(graph);
            this.type = type;
            this.setVerbose(false);
        }
        public ConflictType getConflictType() {
            return (this.type);
        }
        @Override
        public String toString() {
            switch (this.type) {
                case READ_WRITE:
                    return ("RW");
                case WRITE_READ:
                    return ("WR");
                case WRITE_WRITE:
                    return ("WW");
                default:
                    return ("??");
            }
        }
    }
    
    public ConflictGraph(Database catalog_db) {
        this(catalog_db, catalog_db.getProcedures());
    }
    
    public ConflictGraph(Database catalog_db, Collection<Procedure> procs) {
        super(catalog_db);
        
        // First we need to construct the graph
        for (Procedure proc0 : procs) {
            ConflictVertex v0 = this.getVertex(proc0);
            if (v0 == null) v0 = new ConflictVertex(proc0);
            assert(v0 != null);
            
            // READ-WRITE
            for (Procedure proc1 : ConflictSetUtil.getReadWriteConflicts(proc0)) {
                if (procs.contains(proc1) == false) continue;
                ConflictVertex v1 = this.getVertex(proc1);
                if (v1 == null) v1 = new ConflictVertex(proc1);
                ConflictEdge e = new ConflictEdge(this, ConflictType.READ_WRITE);
                this.addEdge(e, v0, v1);
                LOG.debug(String.format("%s %s [%s/%s]", e.type, e.toStringPath(this), proc0.getName(), proc1.getName()));
            } // FOR
            
            // WRITE-WRITE
            for (Procedure proc1 : ConflictSetUtil.getWriteWriteConflicts(proc0)) {
                if (procs.contains(proc1) == false) continue;
                ConflictVertex v1 = this.getVertex(proc1);
                if (v1 == null) v1 = new ConflictVertex(proc1);
                ConflictEdge e = new ConflictEdge(this, ConflictType.WRITE_WRITE);
                this.addEdge(e, v0, v1);
                LOG.debug(String.format("%s %s [%s/%s]", e.type, e.toStringPath(this), proc0.getName(), proc1.getName()));
            } // FOR
        }
        
    }
    @Override
    public EdgeType getEdgeType(ConflictEdge e) {
        return EdgeType.DIRECTED;
    }
}
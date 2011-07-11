package edu.brown.graphs.exceptions;

import edu.brown.graphs.AbstractEdge;
import edu.brown.graphs.AbstractGraphElement;
import edu.brown.graphs.AbstractVertex;
import edu.brown.graphs.IGraph;

public class InvalidGraphElementException extends Exception {
    private static final long serialVersionUID = 1L;
    
    private final IGraph<? extends AbstractVertex, ? extends AbstractEdge> graph;
    private final AbstractGraphElement element;
    
    public InvalidGraphElementException(IGraph<? extends AbstractVertex, ? extends AbstractEdge> graph, AbstractGraphElement element, String message) {
        super((element instanceof AbstractEdge ?    String.format("EDGE[%s]", ((AbstractEdge)element).toStringPath(graph)) :
              (element instanceof AbstractVertex) ? String.format("VERTEX[%s]", element.toString()) :
                                                    String.format("????[%s]", element.toString()))
              + " " + message);
        this.graph = graph;
        this.element = element;
    }
    
    @SuppressWarnings("unchecked")
    public <GE extends AbstractGraphElement> GE getElement() {
        return (GE)this.element;
    }
    
    public IGraph<? extends AbstractVertex, ? extends AbstractEdge> getGraph() {
        return this.graph;
    }
}

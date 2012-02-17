package edu.brown.designer;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import edu.brown.graphs.AbstractEdge;
import edu.brown.graphs.IGraph;

public class DesignerEdge extends AbstractEdge {

    public enum Members {
        WEIGHTS,
    }

    private transient Double total_weight = null;

    /**
     * Edge weights are two-dimensional
     */
    private final Map<Integer, Double> weights = new TreeMap<Integer, Double>();
    {
        this.setAttribute(Members.WEIGHTS.name(), this.weights);
    }

    /**
     * Base constructor
     * 
     * @param graph
     * @param vertices
     */
    public DesignerEdge(IGraph<DesignerVertex, DesignerEdge> graph) {
        super(graph);
    }

    /**
     * Copy constructor
     * 
     * @param graph
     * @param copy
     */
    public DesignerEdge(IGraph<DesignerVertex, DesignerEdge> graph, AbstractEdge copy) {
        super(graph, copy);
    }

    public Collection<Double> getWeights() {
        return (this.weights.values());
    }

    public double getTotalWeight() {
        if (this.total_weight == null) {
            this.total_weight = 0d;
            for (double w : this.weights.values())
                this.total_weight += w;
        }
        return (this.total_weight);
    }

    public Double getWeight(int time) {
        return (this.weights.get(time));
    }

    public int getIntervalCount() {
        return (this.weights.size());
    }

    public void setWeight(int time, double weight) {
        assert (time >= 0);
        int size = this.getIntervalCount();
        if (time >= size) {
            for (int i = size; i < time; i++) {
                this.weights.put(i, 0d);
            } // FOR
            this.total_weight = null;
        }
        this.weights.put(time, weight);
    }

    public void addToWeight(DesignerEdge other) {
        for (Integer time : other.weights.keySet()) {
            Double orig = this.weights.get(time);
            if (orig == null)
                orig = 0.0d;
            orig += other.weights.get(time);
            this.weights.put(time, orig);
        } // FOR
    }

    public void addToWeight(int time, double delta) {
        Double weight = this.weights.get(time);
        if (weight != null) {
            delta += weight;
        }
        this.setWeight(time, delta);
    }

    @Override
    public String toString() {
        String ret = super.toString();
        if (this.getVerbose()) {
            ret += String.format(" [%.03f]", this.getTotalWeight());
        }
        return (ret);
    }
}
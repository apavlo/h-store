package edu.brown.gui.catalog;

public class AttributesNode {
    protected final String label;
    protected final String attributes;
    
    public AttributesNode(String label, String attributes) {
        this.label = label;
        this.attributes = attributes;
    }
    
    @Override
    public String toString() {
        return this.label;
    }
    
    public String getAttributes() {
        return this.attributes;
    }
}

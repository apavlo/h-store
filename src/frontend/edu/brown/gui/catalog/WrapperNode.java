package edu.brown.gui.catalog;

import org.voltdb.catalog.CatalogType;

public class WrapperNode {
    protected final CatalogType catalog_obj;
    protected final boolean show_type;
    protected final String label;

    public WrapperNode(CatalogType catalog_obj, boolean show_type, String custom) {
        this.catalog_obj = catalog_obj;
        this.show_type = show_type;

        if (custom == null) {
            String prefix = (this.show_type ? this.catalog_obj.getClass().getSimpleName() + " " : ""); 
            this.label = prefix + this.catalog_obj.getName();
        } else {
            this.label = custom;
        }
    }
    
    public WrapperNode(CatalogType catalog_obj, boolean show_type) {
        this(catalog_obj, show_type, null);
    }
    
    public WrapperNode(CatalogType catalog_obj) {
        this(catalog_obj, false, null);
    }
    
    public WrapperNode(CatalogType catalog_obj, String custom) {
        this(catalog_obj, false, custom);
    }
    
    @Override
    public String toString() {
        return (this.label);
    }
    
    public CatalogType getCatalogType() {
        return (this.catalog_obj);
    }
}

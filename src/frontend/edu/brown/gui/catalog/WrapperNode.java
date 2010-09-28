package edu.brown.gui.catalog;

import org.voltdb.catalog.CatalogType;

public class WrapperNode {
    protected final CatalogType catalog_obj;
    protected final boolean show_type;

    public WrapperNode(CatalogType catalog_obj, boolean show_type) {
        this.catalog_obj = catalog_obj;
        this.show_type = show_type;
    }
    
    public WrapperNode(CatalogType catalog_obj) {
        this(catalog_obj, false);
    }
    
    @Override
    public String toString() {
        return ((this.show_type ? this.catalog_obj.getClass().getSimpleName() + " " : "") + this.catalog_obj.getName());
    }
    
    public CatalogType getCatalogType() {
        return (this.catalog_obj);
    }
}

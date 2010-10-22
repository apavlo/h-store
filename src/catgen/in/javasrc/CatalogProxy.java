package org.voltdb.catalog;

public abstract class CatalogProxy {

    public static void writeCommands(CatalogType catalog_item, StringBuilder buffer) {
        catalog_item.writeCreationCommand(buffer);
        catalog_item.writeFieldCommands(buffer);
    }
    
    public static void set(CatalogType catalog_item, String field, Object value) {
        if (value instanceof String) {
            value = "\"" + value.toString() + "\"";
        }
        catalog_item.set(field, value.toString());
    }
    
}

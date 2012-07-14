/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

/* WARNING: THIS FILE IS AUTO-GENERATED
            DO NOT MODIFY THIS SOURCE
            ALL CHANGES MUST BE MADE IN THE CATALOG GENERATOR */

package org.voltdb.catalog;

import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;

import edu.brown.catalog.CatalogUtil;

/**
 * The base class for all objects in the Catalog. CatalogType instances all
 * have a name and a path (from the root). They have fields and children.
 * All fields are simple types. All children are CatalogType instances.
 *
 */
public abstract class CatalogType implements Comparable<CatalogType> {

    public static class UnresolvedInfo {
        public String path;
    }

    LinkedHashMap<String, Object> m_fields = new LinkedHashMap<String, Object>();
    LinkedHashMap<String, CatalogMap<? extends CatalogType>> m_childCollections = new LinkedHashMap<String, CatalogMap<? extends CatalogType>>();

    String m_path;
    String m_typename;
    CatalogType m_parent;
    CatalogMap<? extends CatalogType> m_parentMap;
    Catalog m_catalog;
    int m_relativeIndex;

    int m_subTreeVersion;
    int m_nodeVersion;
    
    private transient String m_fullName = null;
    private transient String m_toString = null;
    private transient int m_num_fields = 0;
    private transient Class<? extends CatalogType> m_class = null;
    
    /**
     * Add a field and update the count for the number of fields
     * @param key
     * @param val
     */
    protected synchronized final void addField(String key, Object val) {
        this.m_fields.put(key, val);
        this.m_num_fields++;
    }
    
    /**
     * Get the parent of this CatalogType instance
     * @return The parent of this CatalogType instance
     */
    public String getPath() {
        return m_path;
    }

    /**
     * Get the parent of this CatalogType instance
     * @return The name of this CatalogType instance
     */
    public String getTypeName() {
        return m_typename;
    }
    public String getName() {
        return this.getTypeName();
    }

    /**
     * Get the parent of this CatalogType instance
     * @return The parent of this CatalogType instance
     */
    @SuppressWarnings("unchecked")
    public <T extends CatalogType> T getParent() {
        return (T)m_parent;
    }

    /**
    * Get the root catalog object for this item
    * @return The base Catalog object
    */
    public Catalog getCatalog() {
        return m_catalog;
    }

    /**
     * Get the index of this catalog node relative to its
     * siblings
     * @return The index of this CatalogType instance
     */
    public int getRelativeIndex() {
        return m_relativeIndex;
    }

    public int getNodeVersion() {
        return m_nodeVersion;
    }

    public int getSubTreeVersion() {
        return m_subTreeVersion;
    }

    /**
     * Get the set of field names of the fields of this CatalogType
     * @return The set of field names
     */
    public Set<String> getFields() {
        return m_fields.keySet();
    }

    /**
     * Get the value of a field knowing only the name of the field
     * @param field The name of the field being requested
     * @return The field requested or null
     */
    public Object getField(String field) {
        Object ret = null;
        if (m_fields.containsKey(field)) {
            ret = m_fields.get(field);
            if (ret instanceof UnresolvedInfo) {
                return resolve(field, ((UnresolvedInfo) ret).path);
            }
        }
        return ret;
    }

    CatalogType resolve(String field, String path) {
        CatalogType retval = m_catalog.getItemForRef(path);
        m_fields.put(field, retval);
        return retval;
    }

    /**
     * This should only ever be called from CatalogMap.add(); it's my lazy hack
     * to avoid using reflection to instantiate records.
     */
    void setBaseValues(Catalog catalog, CatalogType parent, String path, String name) {
        if ((name == null) || (catalog == null)) {
            throw new CatalogException("Null value where it shouldn't be.");
        }
        m_catalog = catalog;
        m_parent = parent;
        m_path = path;
        m_typename = name;
        m_subTreeVersion = m_catalog.m_currentCatalogVersion;
        m_nodeVersion = m_catalog.m_currentCatalogVersion;
        catalog.registerGlobally(this);
    }

    public abstract void update();
    
    public Set<String> getChildFields() {
        return (m_childCollections.keySet());
    }

    CatalogType addChild(String collectionName, String childName) {
        CatalogMap<? extends CatalogType> map = m_childCollections.get(collectionName);
        if (map == null)
            throw new CatalogException("No collection for name");
        return map.add(childName);
    }

    public CatalogType getChild(String collectionName, String childName) {
        CatalogMap<? extends CatalogType> map = m_childCollections.get(collectionName);
        if (map == null)
            return null;
        return map.get(childName);
    }
    
    public CatalogMap<? extends CatalogType> getChildren(String collectionName) {
        return (m_childCollections.get(collectionName));
    }
    
    public void set(String field, String value) {
        if ((field == null) || (value == null)) {
            throw new CatalogException("Null value where it shouldn't be.");
        }

        if (m_fields.containsKey(field) == false)
            throw new CatalogException("Unexpected field name '" + field + "' for " + this);
        Object current = m_fields.get(field);

        value = value.trim();

        // handle refs
        if (value.startsWith("/")) {
            UnresolvedInfo uinfo = new UnresolvedInfo();
            uinfo.path = value;
            m_fields.put(field, uinfo);
        }
        // null refs
        else if (value.startsWith("null")) {
            m_fields.put(field, null);
        }
        // handle booleans
        else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            if (current.getClass() != Boolean.class)
                throw new CatalogException("Unexpected type for field '" + field + "'.");
            m_fields.put(field, Boolean.parseBoolean(value));
        }
        // handle strings
        else if ((value.startsWith("\"") && value.endsWith("\"")) ||
            (value.startsWith("'") && value.endsWith("'"))) {
            if (current.getClass() != String.class)
                throw new CatalogException("Unexpected type for field.");
            value = value.substring(1, value.length() - 1);
            m_fields.put(field, value);
        }
        // handle ints
        else {
            boolean isint = value.length() > 0;
            for (int i = 0; i < value.length(); i++) {
                if ((i == 0) && (value.length() > 1) && (value.charAt(i) == '-'))
                    continue;
                if (!Character.isDigit(value.charAt(i)))
                    isint = false;
            }
            if (isint) {
                if (current.getClass() != Integer.class)
                    throw new CatalogException("Unexpected type for field.");
                int intValue = Integer.parseInt(value);
                m_fields.put(field, intValue);
            }
            // error
            else {
                throw new CatalogException("Unexpected non-digit character in '" + value + "' for field '" + field + "'");
            }
        }

        update();
        updateVersioning();
    }

    void delete(String collectionName, String childName) {
        if ((collectionName == null) || (childName == null)) {
            throw new CatalogException("Null value where it shouldn't be.");
        }

        if (m_childCollections.containsKey(collectionName) == false)
            throw new CatalogException("Unexpected collection name '" + collectionName + "' for " + this);
        CatalogMap<? extends CatalogType> collection = m_childCollections.get(collectionName);

        collection.delete(childName);
    }

    void updateVersioning() {
        if (m_nodeVersion != m_catalog.m_currentCatalogVersion) {
            m_nodeVersion = m_catalog.m_currentCatalogVersion;
            updateSubTreeVersion();
        }
    }

    void updateSubTreeVersion() {
        if (m_subTreeVersion != m_catalog.m_currentCatalogVersion) {
            m_subTreeVersion = m_catalog.m_currentCatalogVersion;
            if (m_parentMap != null)
                m_parentMap.updateVersioning();
        }
    }

    void writeCreationCommand(StringBuilder sb) {
        // skip root node command
        if (m_path.equals("/"))
            return;

        int lastSlash = m_path.lastIndexOf("/");
        String key = m_path.substring(lastSlash + 1);
        String newPath = m_path.substring(0, lastSlash);
        if (newPath.length() == 0)
            newPath = "/";
        String[] parts = key.split("\\[");
        parts[1] = parts[1].substring(0, parts[1].length() - 1);
        parts[1] = parts[1].trim();

        sb.append("add ").append(newPath).append(" ");
        sb.append(parts[0]).append(" ").append(parts[1]);
        sb.append("\n");
    }

    void writeCommandForField(StringBuilder sb, String field) {
        sb.append("set ").append(m_path).append(" ");
        sb.append(field).append(" ");
        Object value = m_fields.get(field);
        if (value == null) {
            if ((field.equals("partitioncolumn")) && (m_path.equals("/clusters[cluster]/databases[database]/procedures[delivery]")))
                System.out.printf("null for field %s at path %s\n", field, getPath());
            sb.append("null");

        }
        else if (value.getClass() == Integer.class)
            sb.append(value);
        else if (value.getClass() == Boolean.class)
            sb.append(Boolean.toString((Boolean)value));
        else if (value.getClass() == String.class)
            sb.append("\"").append(value).append("\"");
        else if (value instanceof CatalogType)
            sb.append(((CatalogType)value).getPath());
        else if (value instanceof UnresolvedInfo)
            sb.append(((UnresolvedInfo)value).path);
        else
            throw new CatalogException("Unsupported field type '" + value + "'");
        sb.append("\n");
    }

    void writeFieldCommands(StringBuilder sb) {
        for (String field : m_fields.keySet()) {
            writeCommandForField(sb, field);
        }
    }

    void writeChildCommands(StringBuilder sb) {
        for (String childCollection : m_childCollections.keySet()) {
            CatalogMap<? extends CatalogType> map = m_childCollections.get(childCollection);
            map.writeCommandsForMembers(sb);
        }
    }

    public int compareTo(CatalogType o) {
        if (this == o) {
            return 0;
        }
        if (o != null) {
            if (this.m_path == null || o.m_path == null) {
                return (this.hashCode() - o.hashCode());
            } else {
                return this.m_path.compareTo(o.m_path);        
            }
        }
        return (1);
    }

    CatalogType deepCopy(Catalog catalog, CatalogType parent) {

        CatalogType copy = null;
        try {
            copy = getClass().newInstance();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        copy.setBaseValues(catalog, parent, m_path, m_typename);
        copy.m_relativeIndex = m_relativeIndex;
        copy.m_nodeVersion = m_nodeVersion;
        copy.m_subTreeVersion = m_subTreeVersion;

        for (Entry<String, Object> e : m_fields.entrySet()) {
            Object value = e.getValue();
            if (value instanceof CatalogType) {
                CatalogType type = (CatalogType) e.getValue();
                UnresolvedInfo uinfo = new UnresolvedInfo();
                uinfo.path = type.getPath();
                value = uinfo;
            }

            copy.m_fields.put(e.getKey(), e.getValue());
        }

        for (Entry<String, CatalogMap<? extends CatalogType>> e : m_childCollections.entrySet()) {
            CatalogMap<? extends CatalogType> mapCopy = copy.m_childCollections.get(e.getKey());
            mapCopy.copyFrom(e.getValue());
        }

        copy.update();
        catalog.registerGlobally(copy);

        return copy;
    }

    /**
     * Produce a more readable string representation that a simple
     * hash code.
     */
    @Override
    public String toString() {
        if (m_toString == null) {
            m_toString = this.getClass().getSimpleName() + "{" + this.getTypeName() + "}"; 
        }
        return (m_toString);
    }

    /**
     * Return the full name of this CatalogType
     * @return
     */
    public String fullName() {
        if (m_fullName == null) {
            m_fullName = CatalogUtil.getDisplayName(this, false);
        }
        return (m_fullName);
    }
    
    @Override
    public boolean equals(Object obj) {
        // It's null or not a CatalogType
        if (obj == null || (obj instanceof CatalogType) == false) return (false);
        if (this == obj) return (true);
        
        CatalogType other = (CatalogType)obj;
        
        // Are the fields the same value?
        if (this.m_num_fields != other.m_num_fields) return (false);
        
        // Quickly check whether they are at least the same class
        if (this.m_class == null) this.m_class = this.getClass();
        if (other.m_class == null) other.m_class = other.getClass();
        if (this.m_class.equals(other.m_class) == false) return (false);
        
        // SUPER HACK!!!
        // The only thing that we care about matching up correctly by hash code will be database..
        if (this instanceof Database) return (this.hashCode() == other.hashCode());
        
        // Everything else can be about paths...
        // All of this below is busted, so let's just compare paths if we can...
        if (this.m_path == null) {
            String name0 = this.getTypeName();
            String name1 = other.getTypeName();
            if (name0 != null) {
                return (name0.equals(name1));
            } else if (name1 != null) {
                return (false);
            }
            return (other.m_path == null);
        }
        return (this.m_path.equals(other.m_path));
    }
}


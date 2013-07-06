package edu.brown.terminal;

import java.lang.reflect.Field;
import java.util.*;
import java.util.regex.Pattern;

import jline.Completor;

import org.apache.log4j.Logger;
import org.hsqldb.Tokens;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.QueryType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

/**
 * This is a class that holds a list of the table names for an instance of HStore
 * @author gen
 * @author pavlo
 */
public class TokenCompletor implements Completor {
    private static final Logger LOG = Logger.getLogger(TokenCompletor.class);
    private static final LoggerBoolean debug = new LoggerBoolean();

    protected static final String DELIMITER = " ";
    protected static final Pattern SPLIT = Pattern.compile("[" + DELIMITER + "]+");
    
    private static final String TABLE_PREFIXES[] = {
        "UPDATE",
        "FROM",
        "DESCRIBE"
    };
    private static final String COLUMN_PREFIXES[] = {
        "WHERE",
        "SET"
    };
    private static final String PROC_PREFIXES[] = {
        "EXEC"
    };
    
    
    final Catalog catalog;
    
    final SortedSet<String> emptyTokens = new TreeSet<String>();
    final SortedSet<String> allTokens = new TreeSet<String>();
    final SortedSet<String> sqlTokens = new TreeSet<String>();
    final SortedSet<String> specialTokens = new TreeSet<String>();
    final SortedSet<String> commandTokens = new TreeSet<String>();
    final SortedSet<String> tableTokens = new TreeSet<String>();
    final SortedSet<String> columnTokens = new TreeSet<String>();
    final SortedSet<String> procTokens = new TreeSet<String>();
    
    final Set<String> tablePrefixes = new HashSet<String>();
    final Set<String> columnPrefixes = new HashSet<String>();
    final Set<String> procPrefixes = new HashSet<String>();
    
    final Map<String, String> tokenCaseSensitive = new HashMap<String, String>();
    
    public TokenCompletor(Catalog catalog) throws Exception {
        this.catalog = catalog;
        Database catalog_db = CatalogUtil.getDatabase(catalog);
        
        // Core SQL Reserved Words from HSQLDB
        for (int i = 0; i < 1000; i++) {
            if (Tokens.isCoreKeyword(i)) {
                String keyword = Tokens.getKeyword(i);
                if (keyword != null) this.sqlTokens.add(keyword);
            }
        } // FOR

        // Special command tokens
        for (QueryType qtype : QueryType.values()) {
            if (qtype == QueryType.INVALID || qtype == QueryType.NOOP) continue;
            this.commandTokens.add(qtype.name());
        } // FOR
        for (HStoreTerminal.Command c : HStoreTerminal.Command.values()) {
            this.commandTokens.add(c.name());
            this.specialTokens.add(c.name());
        } // FOR
        
        // Catalog Keywords
        // Tables, columns, procedures names
        CollectionUtil.addAll(this.tablePrefixes, TABLE_PREFIXES);
        CollectionUtil.addAll(this.columnPrefixes, COLUMN_PREFIXES);
        for (Table catalog_tbl : CatalogUtil.getDataTables(catalog_db)) {
            this.tableTokens.add(catalog_tbl.getName());
            this.columnPrefixes.add(catalog_tbl.getName() + ".");
            for (Column catalog_col : catalog_tbl.getColumns()) {
                this.columnTokens.add(catalog_col.getName().toUpperCase());
            } // FOR
        } // FOR
        
        CollectionUtil.addAll(this.procPrefixes, PROC_PREFIXES);
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            String procName = catalog_proc.getName().toUpperCase();
            this.tokenCaseSensitive.put(procName, catalog_proc.getName());
            this.procTokens.add(procName);
        } // FOR
        
        this.allTokens.addAll(this.sqlTokens);
        this.allTokens.addAll(this.tableTokens);
        this.allTokens.addAll(this.columnTokens);
        this.allTokens.addAll(this.procTokens);
        
        if (debug.val)
            LOG.debug("Token Information:\n" + this.toString());
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public int complete(String buffer, int cursor, List clist) {
        if (buffer == null || buffer.isEmpty()) {
            clist.addAll(this.commandTokens);
            return (0);
        }
        
        // Find the current token up to the previous space
        int lastIndex = buffer.lastIndexOf(DELIMITER);
        String tokens[] = SPLIT.split(buffer);
        String last = tokens[tokens.length - 1].trim().toUpperCase();
        
        if (debug.val) {
            LOG.debug("BUFFER: '" + buffer + "'");
            LOG.debug("CURSOR: " + cursor);
            LOG.debug("TOKENS: " + Arrays.toString(tokens));
        }
        
        // Figure out whether they are trying to enter in a SQL
        // token, or a name of an element in the database
        String prevToken = null;
        if (buffer.endsWith(DELIMITER) || tokens.length == 1) {
            prevToken = last;
        } else {
            prevToken = tokens[tokens.length-2].toUpperCase();
        }
        prevToken = prevToken.trim().toUpperCase();
        
        if (debug.val) LOG.debug("PREV: " + prevToken);
        
        // The first token must always be a SQL command token
        // Otherwise, check which candidates to use based on
        // what's in the previous token
        SortedSet<String> candidates = (tokens.length == 1 ? this.commandTokens : this.sqlTokens);
        
        // Check whether the buffer is "EXEC <ProcedureName>"
        // If it is, then we don't want to return any tokens for now
        // Ideally we want to be able to print the parameter info for this procedure
        if (tokens.length >= 2 &&
            tokens[0].equalsIgnoreCase(HStoreTerminal.Command.EXEC.name()) &&
            this.procTokens.contains(tokens[1].toUpperCase())) {
            if (debug.val) LOG.debug("EXEC COMMAND: '" + buffer + "'");
            candidates = this.emptyTokens;
        }
        // TABLE
        else if (this.tablePrefixes.contains(prevToken)) {
            if (debug.val) LOG.debug("TABLE PREFIX: '" + prevToken + "'");
            candidates = this.tableTokens;
        }
        // COLUMN
        else if (this.columnPrefixes.contains(prevToken)) {
            if (debug.val) LOG.debug("COLUMN PREFIX: " + prevToken);
            candidates = this.columnTokens;
        }
        // PROCEDURE
        else if (this.procPrefixes.contains(prevToken)) {
            if (debug.val) LOG.debug("PROC PREFIX: " + prevToken);
            candidates = this.procTokens;
        } else if (debug.val) {
            LOG.debug("Using all candidates!");
        }
        assert(candidates != null);
        if (debug.val) LOG.debug("CANDIDATES: " + candidates);
        
        if (buffer.endsWith(DELIMITER)) {
            for (String can : candidates) {
                if (this.tokenCaseSensitive.containsKey(can)) {
                    can = this.tokenCaseSensitive.get(can);
                }
                clist.add(can);
            } // FOR
            
        } else {
            Collection<String> matches = candidates.tailSet(last);
            if (debug.val) LOG.debug("MATCHES: " + matches);
            for (String can : matches) {
                if (debug.val) LOG.debug("Candidate: " + can);
    
                if (can.startsWith(last) == false || can.equalsIgnoreCase(last)) {
                    if (debug.val) LOG.debug("Invalid match!");
                    break;
                }
                
                // Fix capitalization
                if (this.tokenCaseSensitive.containsKey(can)) {
                    can = this.tokenCaseSensitive.get(can);
                }
                
                int index = can.indexOf(DELIMITER, cursor);
                if (index != -1) {
                    can = can.substring(0, index + 1);
                }
                clist.add(can);
            } // FOR
        }

        if (clist.size() == 1) {
            clist.set(0, ((String) clist.get(0)) + " ");
        }

        return (clist.size() == 0 ? -1 : lastIndex+1);
    }
    
    @Override
    public String toString() {
        Class<?> clazz = this.getClass();
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        for (Field f : clazz.getDeclaredFields()) {
            if (f.getName().equalsIgnoreCase("catalog")) continue;
            if (f.getName().equalsIgnoreCase("allTokens")) continue;
            
            Object obj = null;
            try {
                obj = f.get(this);
            } catch (IllegalAccessException ex) {
                throw new RuntimeException(ex);
            }
            m.put(f.getName(), obj);
        } // FOR
        return StringUtil.formatMaps(m);
    }
    
}

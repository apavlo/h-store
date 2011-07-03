package edu.mit.hstore.dtxn;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections15.map.ListOrderedMap;

import edu.brown.utils.Poolable;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ProfileMeasurement.Type;

public class TransactionProfile implements Poolable {

    public static final Field PROFILE_FIELDS[];
    static {
        // Get all of the ProfileMeasurement fields
        Class<TransactionProfile> tsClass = TransactionProfile.class;
        List<Field> fields = new ArrayList<Field>();
        for (Field f : tsClass.getFields()) {
            if (f.getType().equals(ProfileMeasurement.class)) {
                fields.add(f);
            }
        } // FOR
        assert(fields.isEmpty() == false);
        PROFILE_FIELDS = new Field[fields.size()];
        for (int i = 0; i < PROFILE_FIELDS.length; i++) {
            PROFILE_FIELDS[i] = fields.get(i);
        }
    } // STATIC
    
    
    /**
     * Total time spent executing the transaction
     */
    public final ProfileMeasurement total_time = new ProfileMeasurement(Type.TOTAL);
    /**
     * Time spent in HStoreSite procedureInitialization
     */
    public final ProfileMeasurement init_time = new ProfileMeasurement(Type.INITIALIZATION);
    /**
     * Time spent blocked on the initialization latch
     */
    public final ProfileMeasurement blocked_time = new ProfileMeasurement(Type.BLOCKED);
    /**
     * Time spent getting the response back to the client
     */
    public final ProfileMeasurement finish_time = new ProfileMeasurement(Type.CLEANUP);
    /**
     * Time spent waiting in queue
     */
    public final ProfileMeasurement queue_time = new ProfileMeasurement(Type.QUEUE);
    /**
     * The amount of time spent executing the Java-portion of the stored procedure
     */
    public final ProfileMeasurement java_time = new ProfileMeasurement(Type.JAVA);
    /**
     * The amount of time spent coordinating the transaction
     */
    public final ProfileMeasurement coord_time = new ProfileMeasurement(Type.COORDINATOR);
    /**
     * The amount of time spent planning the transaction
     */
    public final ProfileMeasurement planner_time = new ProfileMeasurement(Type.PLANNER);
    /**
     * The amount of time spent executing in the plan fragments
     */
    public final ProfileMeasurement ee_time = new ProfileMeasurement(Type.EE);
    /**
     * The amount of time spent estimating what the transaction will do
     */
    public final ProfileMeasurement estimator_time = new ProfileMeasurement(Type.ESTIMATION);
    
    
    @Override
    public void finish() {
        this.total_time.reset();
        this.init_time.reset();
        this.blocked_time.reset();
        this.queue_time.reset();
        this.finish_time.reset();
        this.java_time.reset();
        this.coord_time.reset();
        this.ee_time.reset();
        this.estimator_time.reset();
    }
    
    @Override
    public boolean isInitialized() {
        return true;
    }
    
    public long[] getTuple() {
        long tuple[] = new long[PROFILE_FIELDS.length];
        for (int i = 0; i < tuple.length; i++) {
            Field f = PROFILE_FIELDS[i];
            try {
                ProfileMeasurement pm = (ProfileMeasurement)f.get(this);
                tuple[i] = pm.getTotalThinkTime();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (i == 0) assert(tuple[i] > 0) : "????";
        } // FOR
        return (tuple);
    }
    
    protected Map<String, Object> debugMap() {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (Field f : PROFILE_FIELDS) {
            Object val = null;
            try {
                val = f.get(this);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            m.put(StringUtil.title(f.getName().replace("_", " ")), val); 
        } // FOR
        return (m);
    }
}

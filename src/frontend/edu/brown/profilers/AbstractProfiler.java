package edu.brown.profilers;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class AbstractProfiler {
    

    protected Collection<ProfileMeasurement> getProfileMeasurements() {
        List<ProfileMeasurement> ret = new ArrayList<ProfileMeasurement>();
        for (Field f : this.getClass().getDeclaredFields()) {
            Object obj = null;
            try {
                obj = f.get(this);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to get value for field '" + f.getName() + "'", ex);
            }
            if (obj instanceof ProfileMeasurement) {
                ret.add((ProfileMeasurement)obj);
            }
        } // FOR
        return (ret);
    }

    /**
     * Reset all of the ProfileMeasurements within this profiler
     */
    public void reset() {
        for (ProfileMeasurement pm : this.getProfileMeasurements()) {
            pm.reset();
        } // FOR
    }
}

package edu.brown.workload;

import java.io.BufferedReader;
import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import edu.brown.statistics.Histogram;
import edu.brown.utils.FileUtil;

public abstract class WorkloadUtil {
    private static final Logger LOG = Logger.getLogger(WorkloadUtil.class);
    
    /**
     * Read a Workload file and generate a Histogram for how often each procedure 
     * is executed in the trace. This is a faster method than having to deserialize the entire
     * workload trace into memory.
     * @param workload_path
     * @return
     * @throws Exception
     */
    public static Histogram getProcedureHistogram(File workload_path) throws Exception {
        final Histogram h = new Histogram();
        final String regex = "^\\{.*?,\"" +
                             AbstractTraceElement.Members.CATALOG_NAME.name() +
                             "\":\"([\\w\\d]+)\",.*";
        final Pattern p = Pattern.compile(regex);

        if (LOG.isDebugEnabled()) LOG.debug("Generating Procedure Histogram from Workload '" + workload_path.getAbsolutePath() + "'");
        BufferedReader reader = FileUtil.getReader(workload_path.getAbsolutePath());
        int line_ctr = 0;
        while (reader.ready()) {
            String line = reader.readLine();
            Matcher m = p.matcher(line);
            assert(m != null) : "Invalid Line #" + line_ctr + " [" + workload_path + "]";
            assert(m.matches()) : "Invalid Line #" + line_ctr + " [" + workload_path + "]";
            if (m.groupCount() > 0) {
                h.put(m.group(1));
            } else {
                LOG.error("Invalid Workload Line: " + line);
                assert(m.groupCount() == 0);
            }
            line_ctr++;
        } // WHILE
        reader.close();
        
        if (LOG.isDebugEnabled()) LOG.debug("Processed " + line_ctr + " workload trace records");
        return (h);
    }
}

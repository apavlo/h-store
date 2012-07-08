/***************************************************************************
 *   Copyright (C) 2011 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.api;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

public class BenchmarkResultsUploader {
    public static final Logger LOG = Logger.getLogger(BenchmarkResultsUploader.class);
    
    private static final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    
    private final URL  m_url;
    
    // The required information for submitting a benchmark result to Codespeed includes:
    private final String field_project;
    private final String field_executable;
    private final String field_commitid;
    private final String field_benchmark;
    private final String field_environment;
    
    // This information can be included but is optional:
    private String  field_std_dev = null;
    private Float   field_min = null;
    private Float   field_max = null;
    private String  field_branch = null;
    private Date    field_result_date = new Date();
    
    /**
     * Constructor 
     * @param url
     * @param project
     * @param executable
     * @param benchmark
     * @param environment
     * @param commitId
     */
    public BenchmarkResultsUploader(URL url, String project, String executable, String benchmark, String environment, String commitId) {
        m_url = url;
        field_project = project;
        field_executable = executable;
        field_benchmark = benchmark;
        field_environment = environment;
        field_commitid = commitId;
    }
    
    public URL getURL() {
        return m_url;
    }
    public String getCommitID() {
        return field_commitid;
    }
    public String getProject() {
        return field_project;
    }
    public String getExcutable() {
        return field_executable;
    }
    public String getBenchmark() {
        return field_benchmark;
    }
    public String getEnvironment() {
        return field_environment;
    }
    public String getStdDev() {
        return field_std_dev;
    }
    public void setStdDev(String mStdDev) {
        field_std_dev = mStdDev;
    }
    public float getMin() {
        return field_min;
    }
    public void setMin(float mMin) {
        field_min = mMin;
    }
    public float getMax() {
        return field_max;
    }
    public void setMax(float mMax) {
        field_max = mMax;
    }
    public String getBranch() {
        return field_branch;
    }
    public void setBranch(String mBranch) {
        field_branch = mBranch;
    }
    public Date getResultDate() {
        return field_result_date;
    }
    public void setResultDate(Date mResultDate) {
        field_result_date = mResultDate;
    }

    /**
     * Upload the given transaction rate to CodeSpeed
     * @param txnrate The transactions executed per second
     */
    public void post(Double txnrate) {
        final boolean debug = LOG.isDebugEnabled();
        StringBuilder sb = new StringBuilder();
        
        // RESULT_VALUE
        try {
            sb.append(String.format("%s=%s", URLEncoder.encode("result_value", "UTF-8"),
                                             URLEncoder.encode(txnrate.toString(), "UTF-8")));
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
        
        // ADDITIONAL PARAMETERS
        for (Field f : this.getClass().getDeclaredFields()) {
            String f_name = f.getName();
            if (f_name.startsWith("field_") == false) continue;
            try {
                Object val = f.get(this);
                if (val == null) continue;
                if (val instanceof Date) val = df.format((Date)val);
                if (sb.length() > 0) sb.append("&");
                sb.append(URLEncoder.encode(f_name.replace("field_", ""), "UTF-8"))
                  .append("=")
                  .append(URLEncoder.encode(val.toString(), "UTF-8"));
            } catch (Exception ex) {
                throw new RuntimeException("Failed to add value for " + f_name, ex); 
            }
        } // FOR
        
        if (debug)
            LOG.debug("Uploading benchmark results to " + m_url);
        try {
            // Send the request
            URLConnection conn = m_url.openConnection();
            conn.setDoOutput(true);
            OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());

            // write parameters
            writer.write(sb.toString());
            writer.flush();

            // Get the response
            StringBuffer answer = new StringBuffer();
            BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                answer.append(line);
            }
            writer.close();
            reader.close();

            // Output the response
            if (debug)
                LOG.debug("Upload Result:\n" + answer.toString());
        } catch (MalformedURLException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

}

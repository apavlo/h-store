/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.benchmark.tpce;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Table;

import edu.brown.utils.TableDataIterable;

/**
 * Java wrapper around the TPC-E EGenLoader executable
 * 
 * @author pavlo
 */
public class EGenLoader {
    private static final Logger LOG = Logger.getLogger(EGenLoader.class);

    protected final File base_path;
    protected final File loader_bin;
    protected final File input_path;
    protected final File output_path;

    // Database Parameters
    private final Integer total_customers;
    private final Integer scale_factor;
    private final Integer initial_workdays;
 
    /**
     * Constructor
     * 
     * @param egenloader_path
     * @param total_customers
     * @param scale_factor
     * @param initial_workdays
     */
    public EGenLoader(String egenloader_path, int total_customers, int scale_factor, int initial_workdays) {
        assert (egenloader_path != null) : "The EGENLOADER_PATH parameter is null";
        assert (!egenloader_path.isEmpty()) : "The EGENLOADER_PATH parameter is empty";
        LOG.debug("egenloader_path: " + egenloader_path);
        this.base_path = new File(egenloader_path);
        this.loader_bin = new File(this.base_path + File.separator + "bin" + File.separator + "EGenLoader");
        if (!this.loader_bin.exists()) {
            LOG.error("Unable to start benchmark. The executable " + this.loader_bin + " does not exist");
            System.exit(1);
        }
        //TODO input_path + flat_in?
        this.input_path = new File(this.base_path + File.separator + "flat_in");
        this.output_path = new File(this.base_path + File.separator + "flat_out");

        this.total_customers = total_customers;
        this.scale_factor = scale_factor;
        this.initial_workdays = initial_workdays;
    }

    /**
     * For a given table catalog object, return an Iterable that will iterate
     * through tuples in the generated flat files.
     * 
     * @param catalog_tbl
     * @return
     * @throws Exception
     */
    public Iterable<Object[]> getTable(final Table catalog_tbl) throws Exception {
        File file = new File(EGenLoader.this.output_path + File.separator + catalog_tbl.getName().toUpperCase() + ".txt");
        return (new TableDataIterable(catalog_tbl, file));
    }

    public Integer getTotalCustomers() {
        return total_customers;
    }

    public Integer getScaleFactor() {
        return scale_factor;
    }

    public Integer getInitialWorkdays() {
        return initial_workdays;
    }

    /**
     * @throws Exception
     */
    protected void generateFixedTables() throws Exception {
        assert (this.loader_bin.exists());

        LOG.debug("Creating fixed sized tables");
        String options[] = new String[] { "-xf" };
        this.execute(options);
    }

    /**
     * @throws Exception
     */
    protected void generateScalingTables(Integer start_customer) throws Exception {
        assert (this.loader_bin.exists());

        LOG.debug("Creating scaling tables");
        String options[] = new String[] { "-xs", "-c", this.total_customers.toString(), "-b", start_customer.toString(), };
        this.execute(options);
    }

    /**
     * @throws Exception
     */
    protected void generateGrowingTables(Integer start_customer) throws Exception {
        assert (this.loader_bin.exists());

        LOG.info("Creating growing tables");
        String options[] = new String[] { "-xg", "-c", this.total_customers.toString(), "-b", start_customer.toString(), };
        this.execute(options);
    }

    /**
     * Clear all the table files in the output path
     * 
     * @throws Exception
     */
    protected void clearTables() throws Exception {
        for (File file : this.output_path.listFiles()) {
            if (file.isFile() && file.getName().endsWith(".txt")) {
                if (!file.delete()) {
                    throw new Exception("Failed to delete file '" + file.getAbsolutePath() + "'");
                }
            }
        } // FOR
        return;
    }

    public File getTablePath(String table_name) {
        return (new File(this.output_path + "/" + table_name.toUpperCase() + ".txt"));
    }

    /**
     * @param options
     * @throws Exception
     */
    protected void execute(String[] options) throws Exception {
        assert (this.loader_bin.exists());

        File working_dir = new File(this.loader_bin.getParent());
        assert (working_dir.exists());

        String base_command[] = new String[] { this.loader_bin.getAbsolutePath(), "-i", this.input_path.getAbsolutePath(), "-o", this.output_path.getAbsolutePath(), "-t",
                this.total_customers.toString(), "-f", this.scale_factor.toString(), "-w", this.initial_workdays.toString(), };
        String command[] = new String[base_command.length + options.length];
        int command_idx = 0;
        for (String c : base_command)
            command[command_idx++] = c;
        String options_debug = "";
        for (String c : options) {
            command[command_idx++] = c;
            options_debug += " " + c;
        } // FOR

        LOG.debug("Executing " + this.loader_bin.getName() + " in " + working_dir);
        LOG.debug("Input Path: " + this.input_path);
        LOG.debug("Output Path: " + this.output_path);
        LOG.debug("Total Customers: " + this.total_customers);
        LOG.debug("Scale Factor: " + this.scale_factor);
        LOG.debug("Initial Workdays: " + this.initial_workdays);
        LOG.debug("Additional Params:" + options_debug);

        //
        // Bombs away!
        //
        String env[] = new String[] {};
        Process proc = null;
        try {
            proc = Runtime.getRuntime().exec(command, env, working_dir);
        } catch (Exception ex) {
            LOG.fatal(ex.getMessage(), ex);
            System.exit(1);
        }
        BufferedReader stdout = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        BufferedReader stderr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));

        //
        // This seems so slutty. There must be a better way... just need to
        // google it
        //
        String line = null;
        while ((line = stdout.readLine()) != null) {
            if (!line.isEmpty())
                LOG.debug(line);
            while ((line = stderr.readLine()) != null) {
                if (!line.isEmpty())
                    LOG.warn(line);
            }
        } // WHILE
        try {
            LOG.debug("Waiting for process to end");
            if (proc.waitFor() != 0) {
                String msg = "Failed to execute " + this.loader_bin.getName() + ". Exit value = " + proc.exitValue();
                LOG.error(msg);
                throw new Exception(msg);
            }
        } catch (InterruptedException e) {
            LOG.error(e);
        }

    }
}

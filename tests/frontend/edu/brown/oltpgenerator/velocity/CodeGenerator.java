package edu.brown.oltpgenerator.velocity;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import edu.brown.oltpgenerator.AbstractBenchmark.AbstractLoader;
import edu.brown.oltpgenerator.AbstractBenchmark.RandomGenerator.AbstractRandomGenerator;
import edu.brown.oltpgenerator.env.ProcEnv;
import edu.brown.oltpgenerator.env.BenchmarkEnv;
import edu.brown.oltpgenerator.env.TableEnv;
import edu.brown.oltpgenerator.env.RandomDistribution.RandomDistributionEnv;
import edu.brown.oltpgenerator.exception.CycleInDagException;

public abstract class CodeGenerator
{

    private static final String         VM_PATH                                = "/Users/zhe/hstore/SVN-Brown/src/tests/frontend/edu/brown/oltpgenerator/velocity";

    private static final String         KEY_PACKAGE_NAME                       = "packageName";
    private static final String         BENCHMARK_PACKAGE_NAME                 = BenchmarkEnv.getPackageName();

    private static final String         KEY_CLASS_NAME                         = "className";

    private static final String         KEY_BENCHMARK_NAME                     = "benchmarkName";
    private static final String         BENCHMARK_NAME                         = BenchmarkEnv.getBenchmarkName();

    private static final String         PROC_FOLDER_NAME                       = "procedures";
    private static final String         PROC_PACKAGE_NAME                      = BENCHMARK_PACKAGE_NAME + "."
                                                                                       + PROC_FOLDER_NAME;
    private static final String         KEY_PROC_PACKAGE_NAME                  = "procPackageName";

    private static final String         KEY_ABSTRACT_BENCHMARK_PACKAGE         = "abstractBenchmark";

    private static final String         KEY_ABSTRACT_BENCHMARK_RANDOM_PACKAGE  = "abstractBenchmarkRandom";

    private static final String         ABSTRACT_BENCHMARK_PACKAGE_NAME        = AbstractLoader.class.getPackage()
                                                                                       .getName();
    private static final String         ABSTRACT_BENCHMARK_RANDOM_PACKAGE_NAME = AbstractRandomGenerator.class
                                                                                       .getPackage().getName();

    private static final String         TABLE_FOLDER_NAME                      = "tables";

    private static final VelocityEngine s_expander                             = initVMEngine();

    public static final String          NO_CSV_PATH                            = "";

    private static VelocityEngine initVMEngine()
    {
        VelocityEngine ret = new VelocityEngine();
        ret.setProperty("file.resource.loader.path", VM_PATH);
        try
        {
            ret.init();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return ret;
    }

    private static void genCode(String macroName, Map<String, Object> bindings, String outputPath)
    {
        // get Macro object
        Template macro;
        try
        {
            macro = s_expander.getTemplate(macroName);
            // create bindings
            VelocityContext env = new VelocityContext();
            for (String key : bindings.keySet())
            {
                env.put(key, bindings.get(key));
            }

            // Output
            FileWriter fw = new FileWriter(outputPath);
            macro.merge(env, fw);
            fw.close();
        }
        catch (ResourceNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (ParseErrorException e)
        {
            e.printStackTrace();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private static void genLoader() throws CycleInDagException
    {
        String macroName = "LoaderMacro.vm";

        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put(KEY_ABSTRACT_BENCHMARK_PACKAGE, ABSTRACT_BENCHMARK_PACKAGE_NAME);
        bindings.put(KEY_PACKAGE_NAME, BENCHMARK_PACKAGE_NAME);
        bindings.put("tblNames", TableEnv.getTableNames(TableEnv.sortTables()));
        bindings.put("schemaFileName", quote(TableEnv.genSchemaFileName()));

        String outputPath = BenchmarkEnv.getProjectPath() + "/Loader.java";

        genCode(macroName, bindings, outputPath);
    }

    private static void genClient()
    {
        String macroName = "ClientMacro.vm";
        String className = BENCHMARK_NAME + "Client";

        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put(KEY_ABSTRACT_BENCHMARK_PACKAGE, ABSTRACT_BENCHMARK_PACKAGE_NAME);
        bindings.put(KEY_PACKAGE_NAME, BENCHMARK_PACKAGE_NAME);
        bindings.put(KEY_PROC_PACKAGE_NAME, PROC_PACKAGE_NAME);
        bindings.put(KEY_CLASS_NAME, className);
        bindings.put("classFile", className + ".class");
        bindings.put("jarFileName", BENCHMARK_NAME + ".jar");
        bindings.put("xacts", ProcEnv.getAllProcedures());

        String outputPath = BenchmarkEnv.getProjectPath() + "/" + className + ".java";
        genCode(macroName, bindings, outputPath);
    }

    private static void genProjectBuilder()
    {
        String macroName = "ProjectBuilderMacro.vm";

        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put(KEY_PACKAGE_NAME, BENCHMARK_PACKAGE_NAME);
        bindings.put(KEY_BENCHMARK_NAME, quote(BENCHMARK_NAME));
        bindings.put("procs", ProcEnv.getAllProcedures());
        bindings.put(KEY_PROC_PACKAGE_NAME, PROC_PACKAGE_NAME);

        String outputPath = BenchmarkEnv.getProjectPath() + "/ProjectBuilder.java";

        genCode(macroName, bindings, outputPath);
    }

    private static void genTables()
    {
        createSubFolder(TABLE_FOLDER_NAME);
        Table[] tables = TableEnv.getAllTables();

        String macroName = "TableMacro.vm";

        for (Table tbl : tables)
        {
            Map<String, Object> bindings = new HashMap<String, Object>();
            bindings.put(KEY_PACKAGE_NAME, BENCHMARK_PACKAGE_NAME + "." + TABLE_FOLDER_NAME);
            bindings.put(KEY_ABSTRACT_BENCHMARK_PACKAGE, ABSTRACT_BENCHMARK_PACKAGE_NAME);
            bindings.put(KEY_ABSTRACT_BENCHMARK_RANDOM_PACKAGE, ABSTRACT_BENCHMARK_RANDOM_PACKAGE_NAME);
            bindings.put("cardinality", TableEnv.getCardinality(tbl.getName()));
            bindings.put(KEY_CLASS_NAME, tbl.getName());
            bindings.put("tblName", quote(tbl.getName()));
            bindings.put("colGenStmts", getColGeneratorStatements(tbl));

            String csvLinkPath = TableEnv.getTableCsvLink(tbl.getName());
            bindings.put("csvLinkPath", csvLinkPath == null ? quote(NO_CSV_PATH) : quote(csvLinkPath));

            String outputPath = BenchmarkEnv.getProjectPath() + "/" + TABLE_FOLDER_NAME + "/" + tbl.getName() + ".java";

            genCode(macroName, bindings, outputPath);
        }
    }

    private static String quote(String name)
    {
        return "\"" + name + "\"";
    }

    private static void genTransactions()
    {
        createSubFolder(PROC_FOLDER_NAME);
        Procedure[] procs = ProcEnv.getAllProcedures();

        String macroName = "ProcMacro.vm";

        int idxXact = 0;
        for (Procedure proc : procs)
        {
            Map<String, Object> bindings = new HashMap<String, Object>();
            bindings.put(KEY_ABSTRACT_BENCHMARK_PACKAGE, ABSTRACT_BENCHMARK_PACKAGE_NAME);
            bindings.put(KEY_ABSTRACT_BENCHMARK_RANDOM_PACKAGE, ABSTRACT_BENCHMARK_RANDOM_PACKAGE_NAME);
            bindings.put(KEY_PACKAGE_NAME, PROC_PACKAGE_NAME);
            bindings.put(KEY_CLASS_NAME, proc.getName());
            bindings.put("paraList", ProcEnv.buildVmParaList(proc));
            bindings.put("idxXact", idxXact++);
            Integer probability = ProcEnv.getProbability(proc.getName());
            bindings.put("probability", probability == null ? 0 : probability);
            bindings.put("paraGenStmts", getParaGeneratorStatements(proc));

            String outputPath = BenchmarkEnv.getProjectPath() + "/" + PROC_FOLDER_NAME + "/" + proc.getName() + ".java";

            genCode(macroName, bindings, outputPath);
        }
    }

    private static void createSubFolder(String procFolderName)
    {
        File procDir = new File(BenchmarkEnv.getProjectPath() + "/" + procFolderName);
        procDir.mkdir();
    }

    private static String[] getColGeneratorStatements(Table tbl)
    {
        Column[] cols = TableEnv.getAllColumns(tbl);
        String[] ret = new String[cols.length];
        for (Column col : cols)
        {
            ret[col.getIndex()] = RandomDistributionEnv.get(col).getRandomGeneratorConstructingStatement();
        }
        return ret;
    }

    private static String[] getParaGeneratorStatements(Procedure proc)
    {
        ProcParameter[] paras = ProcEnv.getAllParas(proc);
        String[] ret = new String[paras.length];
        for (ProcParameter para : paras)
        {
            ret[para.getIndex()] = RandomDistributionEnv.get(para).getRandomGeneratorConstructingStatement();
        }
        return ret;
    }

    public static void bigBang() throws CycleInDagException
    {
        String projectPath = BenchmarkEnv.createEmptyPackage(BenchmarkEnv.getSourceFolderPath(), BenchmarkEnv
                .getPackageName());
        BenchmarkEnv.setProjectPath(projectPath);

        TableEnv.genSchemaFile();
        genTables();
        genLoader();
        genTransactions();
        genClient();
        genProjectBuilder();
    }
}

package edu.brown.oltpgenerator.env;

import java.io.File;
import java.util.StringTokenizer;

import edu.brown.utils.ArgumentsParser;

/**
 * Workload-related environment
 * 
 * @author zhezhang
 * 
 */
public abstract class BenchmarkEnv
{

    private static String       sourceFolderPath = null;

    private static String       projectPath      = null;

    private static String       packageName      = null;

    private static String       workloadName     = null;

    private static final String programRootPath  = BenchmarkEnv.class.getResource("/").getPath();
    
    private static ArgumentsParser s_externalArg;
    
    public static void setExternalArgs(ArgumentsParser args)
    {
        s_externalArg = args;
    }

    public static ArgumentsParser getExternalArgs()
    {
        return s_externalArg;
    }

    public static String getMacroFolderPath()
    {
        String rootFolderName = new File(programRootPath).getName();
        String suffix = "edu/brown/oltpgenerator/";
        // bin: run from Eclipse
        // test: run from Volt command line
        if (rootFolderName.equals("bin") || rootFolderName.equals("test"))
        {
            String voltRoot = programRootPath + "../../../";
            return voltRoot + "tests/frontend/" + suffix;
        }
        else
        {
            throw new RuntimeException("Invalid place: " + rootFolderName);
        }
    }

    public static void setSourceFolderPath(String path)
    {
        sourceFolderPath = path;
    }

    /**
     * @return the path of folder where the project package is placed
     */
    public static String getSourceFolderPath()
    {
        return sourceFolderPath;
    }

    public static void setProjectPath(String path)
    {
        projectPath = path;
    }

    /**
     * @return the path of the root-level contents in the project package
     */
    public static String getProjectPath()
    {
        return projectPath;
    }

    /**
     * @return the name of project package. For example, "edu.brown.hi"
     */
    public static String getPackageName()
    {
        return packageName;
    }

    public static void setPackageName(String name)
    {
        packageName = name;
    }

    public static void setBenchmarkName(String workloadName)
    {
        BenchmarkEnv.workloadName = workloadName;
    }

    public static String getBenchmarkName()
    {
        return workloadName;
    }

    /**
     * Create an empty package in root directory
     * 
     * @param root
     *            the path of root directory
     * @param packageName
     *            name of package
     * @return the path of contents in the packages
     */
    public static String createEmptyPackage(String root, String packageName)
    {
        File base = new File(root);
        assert (base.exists());

        StringTokenizer st = new StringTokenizer(packageName, ".");
        while (st.hasMoreTokens())
        {
            base = new File(base.getPath() + "/" + st.nextToken());
            if (!base.exists())
                base.mkdir();
        }
        return base.getPath();
    }
}
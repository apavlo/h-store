package edu.brown.oltpgenerator.test;

import edu.brown.oltpgenerator.env.BenchmarkEnv;
import edu.brown.oltpgenerator.env.ProcEnv;
import edu.brown.oltpgenerator.env.TableEnv;
import edu.brown.oltpgenerator.velocity.CodeGenerator;
import edu.brown.utils.ArgumentsParser;

public class GenHelloWorldBenchmark
{

    public static void main(String[] args) throws Exception
    {
        BenchmarkEnv.setExternalArgs(ArgumentsParser.load(args));
        
        // set
        BenchmarkEnv.setSourceFolderPath("/Users/zhe/hstore/SVN-Brown/src/tests/frontend");
        BenchmarkEnv.setBenchmarkName("HelloWorld");
        BenchmarkEnv.setPackageName("edu.brown.benchmark.helloworld");
        TableEnv.setSrcSchemaPath("/Users/zhe/tm1-ddl-fkeys.sql");
        // load
        TableEnv.readSchema();
        TableEnv.loadDefaultColumnProperty();
        ProcEnv.loadTraceFile();
        ProcEnv.loadDefaultParaProperty();
        // generate
        CodeGenerator.bigBang();

        System.out.println("Benchmark generated in " + BenchmarkEnv.getProjectPath());
    }

}

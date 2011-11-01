package edu.brown.benchmark;

import edu.brown.benchmark.BenchmarkResults.FinalResult;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.StringUtil;

public class JSONResultsPrinter extends ResultsPrinter {
   
    @Override
    protected String formatFinalResults(BenchmarkResults results) {
        System.out.print(StringUtil.SINGLE_LINE);
        System.out.println("Base Partition Distribution:\n" + results.getBasePartitions());
        System.out.print(StringUtil.SINGLE_LINE);
        
        FinalResult fr = new FinalResult(results);
        return "<json>\n" + JSONUtil.format(fr) + "\n</json>";
    }
}

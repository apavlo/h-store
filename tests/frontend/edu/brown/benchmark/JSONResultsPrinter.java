package edu.brown.benchmark;

import edu.brown.benchmark.BenchmarkResults.FinalResult;
import edu.brown.utils.JSONUtil;

public class JSONResultsPrinter extends ResultsPrinter {
   
    @Override
    protected String formatFinalResults(BenchmarkResults results) {
        FinalResult fr = new FinalResult(results);
        return "<json>\n" + JSONUtil.format(fr) + "\n</json>";
    }
}

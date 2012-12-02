package edu.brown.api;

import edu.brown.api.results.BenchmarkResults;

public interface BenchmarkInterest {
    public String formatFinalResults(BenchmarkResults results);
    public void benchmarkHasUpdated(BenchmarkResults currentResults);
    public void markEvictionStart();
    public void markEvictionStop();
    public void stop();
}